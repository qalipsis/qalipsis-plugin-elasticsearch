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

package io.qalipsis.plugins.elasticsearch.events

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isNotNull
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.every
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventGeoPoint
import io.qalipsis.api.events.EventJsonConverter
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventRange
import io.qalipsis.api.events.EventTag
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.apache.http.HttpHost
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.PrintWriter
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAccessor
import java.util.concurrent.TimeUnit

/**
 * Complex integration test with Elasticsearch containers to validate that the bulk indexation is working and the
 * fields are successfully stored.
 *
 * @author Eric Jessé
 */
@Testcontainers
@Timeout(3, unit = TimeUnit.MINUTES)
internal abstract class AbstractElasticsearchEventsPublisherIntegrationTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    // The meter registry should provide a timer that execute the expressions to record.
    protected val meterRegistry: MeterRegistry = relaxedMockk {
        every { timer(any(), *anyVararg()) } returns relaxedMockk {
            every { record(any<Runnable>()) } answers { (firstArg() as Runnable).run() }
        }
    }

    protected abstract val container: ElasticsearchContainer

    protected abstract val dateFormatter: DateTimeFormatter

    protected abstract val requiresType: Boolean

    protected lateinit var restClient: RestClient

    protected lateinit var configuration: ElasticsearchEventsConfiguration

    protected val eventsConverter = EventJsonConverter()

    @BeforeAll
    internal fun setUp() {
        val url = "http://${container.httpHostAddress}"
        configuration = ElasticsearchEventsConfiguration(
        ).apply {
            urls = listOf(url)
            batchSize = 100
            lingerPeriod = Duration.ofMinutes(10)
            publishers = 1
        }
        restClient = RestClient.builder(HttpHost.create(url)).build()
    }

    @AfterAll
    internal fun tearDown() {
        restClient.close()
    }

    @Test
    @Timeout(30)
    internal fun `should export data`() = testDispatcherProvider.run {
        // given
        val publisher = ElasticsearchEventsPublisher(
            this,
            this.coroutineContext,
            configuration,
            meterRegistry,
            eventsConverter
        )
        publisher.start()

        val events = mutableListOf<Event>()
        events.add(Event(name = "my-event", EventLevel.INFO))
        events.add(
            Event(
                name = "my-event",
                EventLevel.INFO,
                tags = listOf(EventTag("key-1", "value-1"), EventTag("key-2", "value-2"))
            )
        )

        val instantNow = Instant.now().minusSeconds(12)
        val zdtNow = ZonedDateTime.now(Clock.systemUTC().zone)
        val ldtNow = LocalDateTime.now().plusDays(1)
        val values = createTestData(instantNow, zdtNow, ldtNow, dateFormatter)

        val logValues = listOf(*values.keys.toTypedArray())
        logValues.forEachIndexed { index, value ->
            events.add(Event(name = "my-event-$index", EventLevel.INFO, value = value))
        }

        // when
        publisher.coInvokeInvisible<Void>("performPublish", events)

        // then
        val hitsJson = requestEvents()
        val hits = hitsJson.withArray("hits")
        Assertions.assertEquals(logValues.size + 2, hits.size())

        // Verification of the overall values.
        val expectedIndex = "qalipsis-events-${DateTimeFormatter.ofPattern("uuuu-MM-dd").format(zdtNow)}"
        hits.forEach { hit ->
            Assertions.assertEquals(expectedIndex, hit["_index"].asText())
            (hit["fields"] as ObjectNode).apply {
                Assertions.assertEquals("info", this.withArray("level")[0].asText())
                Assertions.assertNotNull(this.withArray("@timestamp")[0].asLong())
            }
        }

        // Verification of the events without values but with tags.
        assertDoesNotThrow("Item with tags should be found") {
            hits.first { item ->
                kotlin.runCatching {
                    (item["fields"] as ObjectNode).withArray("tags.key-1")[0].asText() == "value-1"
                            && (item["fields"] as ObjectNode).withArray("tags.key-2")[0].asText() == "value-2"
                }.getOrDefault(false)
            }
        }

        // Verification of the events with values.
        logValues.forEachIndexed { index, value ->
            val searchCriteria = values.getValue(value)
            assertDoesNotThrow("Item of value $value and type ${value::class} was not found") {
                hits.first { item ->
                    kotlin.runCatching {
                        (item["fields"] as ObjectNode).let { fields ->
                            "my-event-$index" == fields.withArray("name")[0].asText() && searchCriteria(fields)
                        }
                    }.getOrDefault(false)
                }
            }
        }

        publisher.stop()
    }

    @Test
    @Timeout(5)
    internal fun `should throw an exception when the metadata is invalid`() = testDispatcherProvider.run {
        // given
        val publisher = ElasticsearchEventsPublisher(
            this,
            this.coroutineContext,
            configuration,
            meterRegistry,
            eventsConverter
        )
        publisher.start()
        val bulkRequest = Request("POST", "_bulk")
        val metadataLine = if (requiresType) {
            """{"index":{"_index":"qalipsis-events-2021-03-19","_type":"_doc","_id":"216a1b91-4af1-6cba-36ec-36c4ec682c23"}}"""
        } else {
            """{"index":{"_index":"qalipsis-events-2021-03-19","_id":"216a1b91-4af1-6cba-36ec-36c4ec682c23"}}"""
        }
        // The payload should terminate with a new line, but does not.
        bulkRequest.setJsonEntity(
            """$metadataLine
            {"@timestamp":1616167911000,"name":"my-event-4","level":"info","non-finite-decimal":-Infinity"}
        """.trimIndent()
        )

        // when
        assertThrows<ResponseException> {
            publisher.coInvokeInvisible("executeBulk", bulkRequest, System.currentTimeMillis(), 1)
        }

        publisher.stop()
    }

    @Test
    @Timeout(5)
    internal fun `should throw an exception when a document is invalid`() = testDispatcherProvider.run {
        // given
        val publisher = ElasticsearchEventsPublisher(
            this,
            this.coroutineContext,
            configuration,
            meterRegistry,
            eventsConverter
        )
        publisher.start()
        val bulkRequest = Request("POST", "_bulk")
        val metadataLine = if (requiresType) {
            """{"index":{"_index":"qalipsis-events-2021-03-19","_type":"_doc","_id":"216a1b91-4af1-6cba-36ec-36c4ec682c23"}}"""
        } else {
            """{"index":{"_index":"qalipsis-events-2021-03-19","_id":"216a1b91-4af1-6cba-36ec-36c4ec682c23"}}"""
        }
        bulkRequest.setJsonEntity(
            """$metadataLine
{"@timestamp":1616167911000,"name":"my-event-4","level":"info","date":"not an instant"}
        
"""
        )

        // when
        val errorMessage = assertThrows<ElasticsearchException> {
            publisher.coInvokeInvisible<Void>("executeBulk", bulkRequest, System.currentTimeMillis(), 1)
        }.message

        // then
        assertThat(errorMessage).isNotNull().contains("failed to parse field [date] of type [date]")

        publisher.stop()
    }

    /**
     * Create the test data set with the value to log as key and the condition to match when asserting the data as value.
     */
    private fun createTestData(
        instantNow: Instant, zdtNow: ZonedDateTime,
        ldtNow: LocalDateTime,
        formatter: DateTimeFormatter
    ): Map<Any, (json: ObjectNode) -> Boolean> {
        val values = mapOf<Any, ((json: ObjectNode) -> Boolean)>(
            "my-message" to { json -> json.withArray("message")[0].asText() == "my-message" },
            true to { json -> json.withArray("boolean")[0].asBoolean() },
            123.65 to { json -> json.withArray("number")[0].asDouble() == 123.65 },
            Double.POSITIVE_INFINITY to { json ->
                json.withArray("non-finite-decimal")[0].asText() == "Infinity"
            },
            Double.NEGATIVE_INFINITY to { json ->
                json.withArray("non-finite-decimal")[0].asText() == "-Infinity"
            },
            Double.NaN to { json ->
                json.withArray("non-finite-decimal")[0].asText() == "NaN"
            },
            123.65.toFloat() to { json -> json.withArray("number")[0].asDouble() == 123.65 },
            123.65.toBigDecimal() to { json -> json.withArray("number")[0].asDouble() == 123.65 },
            123 to { json -> json.withArray("number")[0].asInt() == 123 },
            123.toBigInteger() to { json -> json.withArray("number")[0].asInt() == 123 },
            123.toLong() to { json -> json.withArray("number")[0].asInt() == 123 },
            instantNow to { json ->
                formatter.parse(
                    json.withArray("date")[0].asText()
                ) { temporal: TemporalAccessor ->
                    Instant.from(temporal)
                } == instantNow.truncatedTo(ChronoUnit.MILLIS)
            },
            zdtNow to { json ->
                formatter.parse(
                    json.withArray("date")[0].asText()
                ) { temporal: TemporalAccessor ->
                    Instant.from(temporal)
                } == zdtNow.toInstant().truncatedTo(ChronoUnit.MILLIS)
            },
            ldtNow to { json ->
                formatter.parse(
                    json.withArray("date")[0].asText()
                ) { temporal: TemporalAccessor ->
                    Instant.from(temporal)
                } == ldtNow.atZone(ZoneId.systemDefault()).toInstant().truncatedTo(ChronoUnit.MILLIS)
            },
            relaxedMockk<Throwable> {
                every { message } returns "my-error"
                every { printStackTrace(any<PrintWriter>()) } answers {
                    (firstArg() as PrintWriter).write("this is the stack")
                }
            } to { json ->
                json.withArray("error")[0].asText() == "my-error" && json.withArray(
                    "stack-trace"
                )[0].asText() == "this is the stack"
            },
            Duration.ofNanos(12_123_456_789) to { json ->
                json.withArray("duration")[0].asDouble() == 12.123_456_789
            },
            EventGeoPoint(12.34, 34.76) to { json ->
                json.withArray("point")[0].asText() == "12.34, 34.76"
            },
            EventRange(12.34, 34.76, includeUpper = false) to { json ->
                val range = json.withArray("numeric-range")[0]
                range.asText() == "[12.34 : 34.76)"
            },
            arrayOf(12.34, 8765, "here is the test", "here is the other test", Duration.ofMillis(123)) to { json ->
                json.withArray("number")[0].asDouble() == 12.34
                        && json.withArray("message")[0].asText() == "here is the test"
                        && json.withArray("duration")[0].asDouble() == 0.123
                        && json.withArray("values").let {
                    it[0].asText() == "8765" && it[1].asText() == "here is the other test"
                }
            },
            listOf(12.34, 8765, "here is the test", "here is the other test", Duration.ofMillis(123)) to { json ->
                json.withArray("number")[0].asDouble() == 12.34
                        && json.withArray("message")[0].asText() == "here is the test"
                        && json.withArray("duration")[0].asDouble() == 0.123
                        && json.withArray("values").let {
                    it[0].asText() == "8765" && it[1].asText() == "here is the other test"
                }
            },
            MyTestObject() to { json ->
                json.withArray("value")[0].asText() == "{\"property1\":1243.65,\"property2\":\"here is the test\"}"
            }
        )
        return values
    }

    private fun requestEvents(): ObjectNode {
        restClient.performRequest(Request("POST", "/_refresh"))
        val searchRequest = Request("GET", "/qalipsis-events/_search?size=100&stored_fields=*")
        val response = EntityUtils.toString(restClient.performRequest(searchRequest).entity)
        return ObjectMapper().readTree(response)["hits"] as ObjectNode
    }

    data class MyTestObject(val property1: Double = 1243.65, val property2: String = "here is the test")
}
