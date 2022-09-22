/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.elasticsearch.save

import assertk.all
import assertk.assertThat
import assertk.assertions.isBetween
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.verify
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.ElasticsearchBulkResponse
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.assertk.typedProp
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import org.apache.http.HttpHost
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit


/**
 * Complex integration test with Elasticsearch containers to validate that the bulk indexation is working and the
 * fields are successfully stored.
 *
 * @author Eric Jessé
 */
@WithMockk
@Testcontainers
@Timeout(3, unit = TimeUnit.MINUTES)
internal abstract class AbstractElasticsearchBulkClientIntegrationTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    protected abstract val container: ElasticsearchContainer

    protected abstract val dateFormatter: DateTimeFormatter

    protected abstract val requiresType: Boolean

    protected lateinit var restClient: RestClient

    protected lateinit var client: ElasticsearchSaveQueryClientImpl

    @RelaxedMockK
    protected lateinit var eventsLogger: EventsLogger

    @RelaxedMockK
    protected lateinit var documentsCount: Counter

    @RelaxedMockK
    protected lateinit var timeToResponseTimer: Timer

    @RelaxedMockK
    protected lateinit var successCounter: Counter

    @RelaxedMockK
    protected lateinit var failureCounter: Counter

    @RelaxedMockK
    protected lateinit var savedBytesCounter: Counter

    protected val jsonMapper = JsonMapper().also {
        it.registerModule(JavaTimeModule())
        it.registerModule(kotlinModule { })
        it.registerModule(Jdk8Module())
    }

    @BeforeAll
    fun setUpAll() {
        val url = "http://${container.httpHostAddress}"
        restClient = RestClient.builder(HttpHost.create(url)).build()
    }

    @AfterEach
    fun tearDown() {
        restClient.performRequest(Request("DELETE", "/_all"))
    }

    @AfterAll
    fun tearDownAll() {
        restClient.close()
    }

    @Test
    @Timeout(30)
    fun `should export data`() = testDispatcherProvider.run {
        val metersTags = relaxedMockk<Tags>()
        val meterRegistry = relaxedMockk<MeterRegistry> {
            every { counter("elasticsearch-save-received-documents", refEq(metersTags)) } returns documentsCount
            every { timer("elasticsearch-save-time-to-response", refEq(metersTags)) } returns timeToResponseTimer
            every { counter("elasticsearch-save-successes", refEq(metersTags)) } returns successCounter
            every { counter("elasticsearch-save-failures", refEq(metersTags)) } returns failureCounter
            every { counter("elasticsearch-save-success-bytes", refEq(metersTags)) } returns savedBytesCounter
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
        }

        // given
        val client = ElasticsearchSaveQueryClientImpl(
            ioCoroutineScope = this,
            clientBuilder = { restClient },
            jsonMapper = jsonMapper,
            keepElasticsearchBulkResponse = true,
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )
        client.start(startStopContext)

        val tags: Map<String, String> = emptyMap()
        val documents = listOf(
            Document("index1", "_doc", null, """{"query": "data1","count": 7}"""),
            Document("index2", "_doc", null, """{"query": "data2","count": 7}""")
        )
        val resultOfExecute = client.execute(documents, tags)
        assertThat(resultOfExecute).isInstanceOf(ElasticsearchBulkResult::class.java).all {
            prop("meters").isNotNull().isInstanceOf(ElasticsearchBulkMeters::class.java).all {
                prop("savedDocuments").isEqualTo(2)
                prop("failedDocuments").isEqualTo(0)
                prop("timeToResponse").isNotNull().isInstanceOf(Duration::class.java)
                typedProp<Long>("bytesToSave").isNotNull().isBetween(433L, 464L)
                prop("documentsToSave").isEqualTo(2)
            }
            prop("responseBody").isNotNull().isInstanceOf(ElasticsearchBulkResponse::class.java).all {
                prop("httpStatus").isEqualTo(200)
                prop("responseBody").isNotNull()
            }
        }

        val retrievalPayload = refreshIndicesAndFetchAllDocuments()
        val sortedHits = retrievalPayload.withArray("hits").toMutableList().sortedBy { it.get("_index").asText() }
        var counter = 1
        sortedHits.forEach {
            (it["_source"] as ObjectNode).apply {
                Assertions.assertEquals("data${counter}", this.get("query").asText())
                Assertions.assertEquals("7", this.get("count").asText())
            }
            Assertions.assertEquals("index${counter}", it["_index"].asText())
            counter++
        }
        Assertions.assertEquals(2, sortedHits.size)

        verify {
            documentsCount.increment(2.0)
            timeToResponseTimer.record(more(0L), TimeUnit.NANOSECONDS)
            successCounter.increment(2.0)
            savedBytesCounter.increment(withArg { assertThat(it).isBetween(433.0, 464.0) })
        }
        confirmVerified(documentsCount, timeToResponseTimer, successCounter, savedBytesCounter)
    }

    @Test
    @Timeout(30)
    fun `should generate failure when some documents are invalid JSON`() = testDispatcherProvider.run {
        val metersTags = relaxedMockk<Tags>()
        val meterRegistry = relaxedMockk<MeterRegistry> {
            every { counter("elasticsearch-save-received-documents", refEq(metersTags)) } returns documentsCount
            every { timer("elasticsearch-save-time-to-response", refEq(metersTags)) } returns timeToResponseTimer
            every { counter("elasticsearch-save-successes", refEq(metersTags)) } returns successCounter
            every { counter("elasticsearch-save-failures", refEq(metersTags)) } returns failureCounter
            every { counter("elasticsearch-save-success-bytes", refEq(metersTags)) } returns savedBytesCounter
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
        }

        // given
        val client = ElasticsearchSaveQueryClientImpl(
            ioCoroutineScope = this,
            clientBuilder = { restClient },
            jsonMapper = jsonMapper,
            keepElasticsearchBulkResponse = true,
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )
        client.start(startStopContext)

        val tags: Map<String, String> = emptyMap()
        val documents = listOf(
            Document("index1", "_doc", null, """{"query": "data1","count": girl}"""),
            Document("index2", "_doc", null, """{"query": "data1","count": asa}"""),
            Document("index1", "_doc", null, """{"query": "data2","count": 7}""")
        )

        //when
        val errorMessage = assertThrows<ElasticsearchException> {
            client.execute(documents, tags)
        }.message

        //then
        assertThat(errorMessage).isNotNull()
        verify {
            documentsCount.increment(3.0)
            failureCounter.increment(2.0)
            timeToResponseTimer.record(more(0L), TimeUnit.NANOSECONDS)
            successCounter.increment(1.0)
        }
        confirmVerified(documentsCount, timeToResponseTimer, successCounter, failureCounter)
    }

    private fun refreshIndicesAndFetchAllDocuments(): ObjectNode {
        restClient.performRequest(Request("POST", "/_refresh"))
        val searchRequest = Request("GET", "/_search?size=3")
        val response = EntityUtils.toString(restClient.performRequest(searchRequest).entity)
        return ObjectMapper().readTree(response)["hits"] as ObjectNode
    }
}
