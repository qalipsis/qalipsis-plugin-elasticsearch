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
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.ElasticsearchBulkResponse
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.apache.http.HttpHost
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
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

    private val eventsLogger = relaxedMockk<EventsLogger>()

    private val documentsCount = relaxedMockk<Counter>()

    private var timeToResponseTimer = relaxedMockk<Timer>()

    private var successCounter = relaxedMockk<Counter>()

    private var failureCounter = relaxedMockk<Counter>()

    private var savedBytesCounter = relaxedMockk<Counter>()

    private var failureBytesCounter = relaxedMockk<Counter>()

    protected val jsonMapper = JsonMapper().also {
        it.registerModule(JavaTimeModule())
        it.registerModule(KotlinModule())
        it.registerModule(Jdk8Module())
    }

    @BeforeEach
    internal fun setUp() {
        val url = "http://${container.httpHostAddress}"
        restClient = RestClient.builder(HttpHost.create(url)).build()
    }

    @AfterEach
    internal fun tearDown() {
        restClient.close()
    }

    @Test
    @Timeout(30)
    internal fun `should export data`() = testDispatcherProvider.run {
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
                prop("bytesToSave").isNotNull().isEqualTo(463L)
                prop("documentsToSave").isEqualTo(2)
            }
            prop("responseBody").isNotNull().isInstanceOf(ElasticsearchBulkResponse::class.java).all {
                prop("httpStatus").isEqualTo(200)
                prop("responseBody").isNotNull()
            }
        }

        val retrievalPayload = refreshIndicesAndFetchAllDocuments()
        val sortedHits = retrievalPayload.withArray("hits").toMutableList()
        sortedHits.sortBy { it.get("_index").asText() }
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
            savedBytesCounter.increment(463.0)
        }
        confirmVerified(documentsCount, timeToResponseTimer, successCounter, savedBytesCounter)

        client.stop(startStopContext)
    }

    @Test
    @Timeout(5)
    internal fun `should throw an exception when a document is invalid`() = testDispatcherProvider.run {
        val metersTags = relaxedMockk<Tags>()
        val meterRegistry = relaxedMockk<MeterRegistry> {
            every { counter("elasticsearch-save-received-documents", refEq(metersTags)) } returns documentsCount
            every { timer("elasticsearch-save-time-to-response", refEq(metersTags)) } returns timeToResponseTimer
            every { counter("elasticsearch-save-successes", refEq(metersTags)) } returns successCounter
            every { counter("elasticsearch-save-failures", refEq(metersTags)) } returns failureCounter
            every { counter("elasticsearch-save-success-bytes", refEq(metersTags)) } returns savedBytesCounter
            every { counter("elasticsearch-save-failure-bytes", refEq(metersTags)) } returns failureBytesCounter
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
        }

        // given
        val client = ElasticsearchSaveQueryClientImpl(
            ioCoroutineScope = this,
            clientBuilder = { restClient },
            jsonMapper = jsonMapper,
            keepElasticsearchBulkResponse = false,
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )
        client.start(startStopContext)

        val tags: Map<String, String> = emptyMap()

        val documents = listOf(
            Document("key1", "val1", null, """"query": "data1"""),
            Document("index", "_doc", null, """{"query": "data3","count": 7}""")
        )
        val resultOfExecute = client.execute(documents, tags)
        assertThat(resultOfExecute).isInstanceOf(ElasticsearchBulkResult::class.java).all {
            prop("meters").isNotNull().isInstanceOf(ElasticsearchBulkMeters::class.java).all {
                prop("savedDocuments").isEqualTo(1)
                prop("failedDocuments").isEqualTo(1)
                prop("timeToResponse").isNotNull().isInstanceOf(Duration::class.java)
                prop("documentsToSave").isEqualTo(2)
            }
            prop("responseBody").isNull()
        }
        val retrievalPayload = refreshIndicesAndFetchAllDocuments()
        val hits = retrievalPayload.withArray("hits")
        Assertions.assertEquals(1, hits.size())
        hits.forEach { item ->
            (item["_source"] as ObjectNode).apply {
                Assertions.assertEquals("data3", this.get("query").asText())
                Assertions.assertEquals("7", this.get("count").asText())
            }
            Assertions.assertEquals("index", item["_index"].asText())
        }

        restClient.performRequest(Request("DELETE", "/index/_doc/${hits.get(0).get("_id").asText()}"))

        verify {
            documentsCount.increment(2.0)
            timeToResponseTimer.record(more(0L), TimeUnit.NANOSECONDS)
            failureCounter.increment(1.0)
            successCounter.increment(1.0)
        }
        confirmVerified(documentsCount, timeToResponseTimer, failureCounter, successCounter)

        client.stop(startStopContext)
    }

    private fun refreshIndicesAndFetchAllDocuments(): ObjectNode {
        restClient.performRequest(Request("POST", "/_refresh"))
        val searchRequest = Request("GET", "/_search?size=3")
        val response = EntityUtils.toString(restClient.performRequest(searchRequest).entity)
        return ObjectMapper().readTree(response)["hits"] as ObjectNode
    }
}
