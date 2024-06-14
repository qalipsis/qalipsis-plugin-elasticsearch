/*
 * Copyright 2024 AERIS IT Solutions GmbH
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

package io.qalipsis.plugins.elasticsearch.monitoring.meters

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.hasSize
import assertk.assertions.isNotNull
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.DistributionSummary
import io.qalipsis.api.meters.Gauge
import io.qalipsis.api.meters.MeasurementMetric
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.meters.Statistic
import io.qalipsis.api.meters.Timer
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.plugins.elasticsearch.monitoring.meters.catadioptre.elasticsearchOperations
import io.qalipsis.test.coroutines.TestDispatcherProvider
import org.apache.http.HttpHost
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant
import java.util.concurrent.TimeUnit

/**
 * Complex integration test with Elasticsearch containers to validate that the bulk indexation is working and the
 * fields are successfully stored.
 *
 * @author Francisca Eze
 */
@Testcontainers
@Timeout(3, unit = TimeUnit.MINUTES)
internal abstract class AbstractElasticsearchMeasurementPublisherIntegrationTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    protected abstract val container: ElasticsearchContainer

    protected abstract val requiresType: Boolean

    protected lateinit var restClient: RestClient

    protected lateinit var configuration: ElasticsearchMeasurementConfiguration

    @BeforeAll
    internal fun setUp() {
        val url = "http://${container.httpHostAddress}"
        configuration = ElasticsearchMeasurementConfiguration(
        ).apply {
            urls = listOf(url)
            publishers = 1
            storeSource = true
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
        val publisher = ElasticsearchMeasurementPublisher(
            this,
            configuration
        )
        publisher.init()

        // when
        publisher.coInvokeInvisible<Void>("performPublish", createData())
        publisher.stop()

        // then
        val hitsJson = requestEvents()
        val hits = hitsJson.withArray("hits")

        // Verification of the counter values.
        val countHit = hits[0]["_source"] as ObjectNode
        assertEquals("counter", countHit["@type"].asText())
        assertEquals("my-counter", countHit["name"].asText())
        assertNotNull(countHit["@timestamp"].asLong())
        assertEquals("count", countHit["metrics"][0]["statistic"].asText())
        assertEquals(9.0, countHit["metrics"][0]["value"].asDouble())

        // Verification of the gauge values.
        val gaugeHit = hits[1]["_source"] as ObjectNode
        assertEquals("gauge", gaugeHit["@type"].asText())
        assertEquals("bar", gaugeHit["tags"]["foo"].asText())
        assertEquals("one", gaugeHit["tags"]["cafe"].asText())
        assertEquals("my-gauge", gaugeHit["name"].asText())
        assertNotNull(gaugeHit["@timestamp"].asLong())
        assertEquals("value", gaugeHit["metrics"][0]["statistic"].asText())
        assertEquals(5.0, gaugeHit["metrics"][0]["value"].asDouble())

        // Verification of the timer values.
        val timerHit = hits[2]["_source"] as ObjectNode
        assertEquals("timer", timerHit["@type"].asText())
        assertEquals("my-timer", timerHit["name"].asText())
        assertNotNull(timerHit["@timestamp"].asLong())
        assertEquals("mean", timerHit["metrics"][0]["statistic"].asText())
        assertEquals(224.0, timerHit["metrics"][0]["value"].asDouble())
        assertEquals("total_time", timerHit["metrics"][1]["statistic"].asText())
        assertEquals(178713.0, timerHit["metrics"][1]["value"].asDouble())
        assertEquals("max", timerHit["metrics"][2]["statistic"].asText())
        assertEquals(54328.5, timerHit["metrics"][2]["value"].asDouble())
        assertEquals("percentile", timerHit["metrics"][3]["statistic"].asText())
        assertEquals(85.0, timerHit["metrics"][3]["percentile"].asDouble())
        assertEquals(548.5, timerHit["metrics"][3]["value"].asDouble())
        assertEquals("percentile", timerHit["metrics"][4]["statistic"].asText())
        assertEquals(50.0, timerHit["metrics"][4]["percentile"].asDouble())
        assertEquals(54328.5, timerHit["metrics"][4]["value"].asDouble())

        // Verification of the summary values.
        val summaryHit = hits[3]["_source"] as ObjectNode
        assertEquals("summary", summaryHit["@type"].asText())
        assertEquals("host", summaryHit["tags"]["local"].asText())
        assertEquals("summary", summaryHit["tags"]["dist"].asText())
        assertEquals("my-final-summary", summaryHit["name"].asText())
        assertNotNull(summaryHit["@timestamp"].asLong())
        assertEquals("count", summaryHit["metrics"][0]["statistic"].asText())
        assertEquals(70.0, summaryHit["metrics"][0]["value"].asDouble())
        assertEquals("total", summaryHit["metrics"][1]["statistic"].asText())
        assertEquals(17873213.0, summaryHit["metrics"][1]["value"].asDouble())
        assertEquals("max", summaryHit["metrics"][2]["statistic"].asText())
        assertEquals(548.5, summaryHit["metrics"][2]["value"].asDouble())
        assertEquals("percentile", summaryHit["metrics"][3]["statistic"].asText())
        assertEquals(45.0, summaryHit["metrics"][3]["percentile"].asDouble())
        assertEquals(54.5, summaryHit["metrics"][3]["value"].asDouble())
        assertEquals("percentile", summaryHit["metrics"][4]["statistic"].asText())
        assertEquals(74.5, summaryHit["metrics"][4]["percentile"].asDouble())
        assertEquals(548.5, summaryHit["metrics"][4]["value"].asDouble())

        // Test the search by statistics type.
        val searchRequest = Request("GET", "/qalipsis-meters/_search?size=100")
        searchRequest.setJsonEntity(
            """
            {
              "query": {
                "nested": {
                  "path": "metrics",
                  "query": {
                    "term": {
                      "metrics.statistic": {
                        "value": "count"
                      }
                    }
                  }
                }
              }
            }
        """.trimIndent()
        )
        val response = EntityUtils.toString(restClient.performRequest(searchRequest).entity)
        val countTypeHits = (ObjectMapper().readTree(response)["hits"] as ObjectNode)
            .let { hits -> hits.get("hits") as ArrayNode }.toList()
        assertThat(countTypeHits).hasSize(2)
    }

    @Test
    @Timeout(5)
    internal fun `should throw an exception when the metadata is invalid`() = testDispatcherProvider.run {
        // given
        val publisher = ElasticsearchMeasurementPublisher(this, configuration)
        publisher.init()
        val bulkRequest = Request("POST", "_bulk")
        val metadataLine = if (requiresType) {
            """{"index":{"_index":"qalipsis-meters-2021-03-19","_type":"_doc","_id":"216a1b91-4af1-6cba-36ec-36c4ec682c23"}}"""
        } else {
            """{"index":{"_index":"qalipsis-meters-2021-03-19","_id":"216a1b91-4af1-6cba-36ec-36c4ec682c23"}}"""
        }
        // The payload should terminate with a new line, but does not.
        bulkRequest.setJsonEntity(
            """$metadataLine
            {"@timestamp":1616167911000,"name":"my-counter4","type":"counter","metrics":[{"statistic: "count", "value":8.0}]"}
        """.trimIndent()
        )

        // when
        assertThrows<ResponseException> {
            publisher.elasticsearchOperations().executeBulk(
                bulkRequest,
                System.currentTimeMillis(),
                1,
                null,
                coroutineContext,
                "meters"
            )
        }

        publisher.stop()
    }

    @Test
    @Timeout(5)
    internal fun `should throw an exception when a document is invalid`() = testDispatcherProvider.run {
        // given
        val publisher = ElasticsearchMeasurementPublisher(
            this,
            configuration
        )
        publisher.init()
        val bulkRequest = Request("POST", "_bulk")
        val metadataLine = if (requiresType) {
            """{"index":{"_index":"qalipsis-meters-2021-03-19","_type":"_doc","_id":"216a1b91-4af1-6cba-36ec-36c4ec682c23"}}"""
        } else {
            """{"index":{"_index":"qalipsis-meters-2021-03-19","_id":"216a1b91-4af1-6cba-36ec-36c4ec682c23"}}"""
        }
        bulkRequest.setJsonEntity(
            """$metadataLine
{"@timestamp":1616167911000,"name":"my-counter4","type":"counter","metrics":[{"statistic":"count","value":"Not a float"}]"}

"""
        )

        // when
        val errorMessage = assertThrows<ElasticsearchException> {
            publisher.elasticsearchOperations().executeBulk(
                bulkRequest,
                System.currentTimeMillis(),
                1,
                null,
                coroutineContext,
                "meters"
            )
        }.message

        // then
        assertThat(errorMessage).isNotNull()
            .contains("failed to parse field [metrics.value] of type [double] in document with id '216a1b91-4af1-6cba-36ec-36c4ec682c23'")

        publisher.stop()
    }

    private fun createData(): List<MeterSnapshot<*>> {
        val counterMock = mockk<Counter> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my counter"
                every { tags } returns emptyMap()
                every { type } returns MeterType.COUNTER
                every { scenarioName } returns "SCENARIO one"
                every { campaignKey } returns "first campaign 5473653"
                every { stepName } returns "step uno"
            }
        }
        val timerMock = mockk<Timer> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my timer"
                every { tags } returns emptyMap()
                every { type } returns MeterType.TIMER
                every { scenarioName } returns "SCENARIO two"
                every { campaignKey } returns "second campaign 47628233"
                every { stepName } returns "step dos"
            }
        }
        val gaugeMock = mockk<Gauge> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my gauge"
                every { tags } returns mapOf("foo" to "bar", "cafe" to "one")
                every { type } returns MeterType.GAUGE
                every { scenarioName } returns "SCENARIO three"
                every { campaignKey } returns "third CAMPAIGN 7624839"
                every { stepName } returns "step tres"
            }
        }
        val summaryMock = mockk<DistributionSummary> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my final summary"
                every { tags } returns mapOf("dist" to "summary", "local" to "host")
                every { type } returns MeterType.DISTRIBUTION_SUMMARY
                every { scenarioName } returns "scenario four"
                every { campaignKey } returns "fourth CAMPAIGN 283239"
                every { stepName } returns "step quart"
            }
        }
        val now = Instant.now()
        val countSnapshot = mockk<MeterSnapshot<Counter>> {
            every { timestamp } returns now
            every { meter } returns counterMock
            every { measurements } returns listOf(MeasurementMetric(9.0, Statistic.COUNT))
        }
        val gaugeSnapshot = mockk<MeterSnapshot<Gauge>> {
            every { timestamp } returns now
            every { meter } returns gaugeMock
            every { measurements } returns listOf(MeasurementMetric(5.0, Statistic.VALUE))
        }
        val timerSnapshot = mockk<MeterSnapshot<Timer>> {
            every { timestamp } returns now
            every { meter } returns timerMock
            every { measurements } returns listOf(
                MeasurementMetric(224.0, Statistic.MEAN),
                MeasurementMetric(178713.0, Statistic.TOTAL_TIME),
                MeasurementMetric(54328.5, Statistic.MAX),
                DistributionMeasurementMetric(548.5, Statistic.PERCENTILE, 85.0),
                DistributionMeasurementMetric(54328.5, Statistic.PERCENTILE, 50.0),
            )
        }
        val summarySnapshot = mockk<MeterSnapshot<DistributionSummary>> {
            every { timestamp } returns now
            every { meter } returns summaryMock
            every { measurements } returns listOf(
                MeasurementMetric(70.0, Statistic.COUNT),
                MeasurementMetric(17873213.0, Statistic.TOTAL),
                MeasurementMetric(548.5, Statistic.MAX),
                DistributionMeasurementMetric(54.5, Statistic.PERCENTILE, 45.0),
                DistributionMeasurementMetric(548.5, Statistic.PERCENTILE, 74.5),
            )
        }
        return listOf(countSnapshot, gaugeSnapshot, timerSnapshot, summarySnapshot)
    }

    private fun requestEvents(): ObjectNode {
        restClient.performRequest(Request("POST", "/_refresh"))
        val searchRequest = Request("GET", "/qalipsis-meters/_search?size=100")
        val response = EntityUtils.toString(restClient.performRequest(searchRequest).entity)
        return ObjectMapper().readTree(response)["hits"] as ObjectNode
    }
}
