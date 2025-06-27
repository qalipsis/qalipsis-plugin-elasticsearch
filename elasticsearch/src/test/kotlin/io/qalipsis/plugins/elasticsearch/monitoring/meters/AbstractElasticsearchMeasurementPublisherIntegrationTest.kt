/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.elasticsearch.monitoring.meters

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.contains
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.MeasurementMetric
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.meters.Statistic
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.plugins.elasticsearch.monitoring.meters.catadioptre.elasticsearchOperations
import io.qalipsis.test.coroutines.TestDispatcherProvider
import org.apache.http.HttpHost
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.AfterAll
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
        assertThat(hitsJson.withArray("hits")).all {
            // Verification of the counter values.
            any { hit ->
                hit.transform { it["_source"] as ObjectNode }.all {
                    transform { it.size() }.isEqualTo(5)
                    transform { it["name"].asText() }.isEqualTo("my-counter")
                    transform { it["@type"].asText() }.isEqualTo("counter")
                    transform { it["@timestamp"].asLong() }.isNotNull()
                    transform { it["metrics"] }.all {
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("count")
                            it.transform { it["value"].asInt() }.isEqualTo(9)
                        }
                    }
                    transform { it["tags"] as ObjectNode }.all {
                        transform { it.size() }.isEqualTo(3)
                        transform { it["campaign"].asText() }.isEqualTo("first campaign 5473653")
                        transform { it["scenario"].asText() }.isEqualTo("first scenario")
                        transform { it["step"].asText() }.isEqualTo("step number one")
                    }
                }
            }
            // Verification of the gauge values.
            any { hit ->
                hit.transform { it["_source"] as ObjectNode }.all {
                    transform { it.size() }.isEqualTo(5)
                    transform { it["name"].asText() }.isEqualTo("my-gauge")
                    transform { it["@type"].asText() }.isEqualTo("gauge")
                    transform { it["@timestamp"].asLong() }.isNotNull()
                    transform { it["metrics"] }.all {
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("value")
                            it.transform { it["value"].asInt() }.isEqualTo(5)
                        }
                    }
                    transform { it["tags"] as ObjectNode }.all {
                        transform { it.size() }.isEqualTo(5)
                        transform { it["campaign"].asText() }.isEqualTo("third CAMPAIGN 7624839")
                        transform { it["scenario"].asText() }.isEqualTo("third scenario")
                        transform { it["step"].asText() }.isEqualTo("step number three")
                        transform { it["foo"].asText() }.isEqualTo("bar")
                        transform { it["any-tag"].asText() }.isEqualTo("one")
                    }
                }
            }
            // Verification of the timer values.
            any { hit ->
                hit.transform { it["_source"] as ObjectNode }.all {
                    transform { it.size() }.isEqualTo(5)
                    transform { it["name"].asText() }.isEqualTo("my-timer")
                    transform { it["@type"].asText() }.isEqualTo("timer")
                    transform { it["@timestamp"].asLong() }.isNotNull()
                    transform { it["metrics"] }.all {
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("count")
                            it.transform { it["value"].asInt() }.isEqualTo(80)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("mean")
                            it.transform { it["value"].asInt() }.isEqualTo(224)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("total_time")
                            it.transform { it["value"].asInt() }.isEqualTo(178713)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("max")
                            it.transform { it["value"].asDouble() }.isEqualTo(54328.5)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("percentile")
                            it.transform { it["percentile"].asDouble() }.isEqualTo(85.0)
                            it.transform { it["value"].asDouble() }.isEqualTo(548.5)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("percentile")
                            it.transform { it["percentile"].asDouble() }.isEqualTo(50.0)
                            it.transform { it["value"].asDouble() }.isEqualTo(54328.5)
                        }
                    }
                    transform { it["tags"] as ObjectNode }.all {
                        transform { it.size() }.isEqualTo(3)
                        transform { it["campaign"].asText() }.isEqualTo("second campaign 47628233")
                        transform { it["scenario"].asText() }.isEqualTo("second scenario")
                        transform { it["step"].asText() }.isEqualTo("step number two")
                    }
                }
            }
            // Verification of the summary values.
            any { hit ->
                hit.transform { it["_source"] as ObjectNode }.all {
                    transform { it.size() }.isEqualTo(5)
                    transform { it["name"].asText() }.isEqualTo("my-final-summary")
                    transform { it["@type"].asText() }.isEqualTo("summary")
                    transform { it["@timestamp"].asLong() }.isNotNull()
                    transform { it["metrics"] }.all {
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("count")
                            it.transform { it["value"].asInt() }.isEqualTo(70)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("mean")
                            it.transform { it["value"].asInt() }.isEqualTo(22)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("total")
                            it.transform { it["value"].asDouble() }.isEqualTo(1.7873213E7)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("max")
                            it.transform { it["value"].asDouble() }.isEqualTo(548.5)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("percentile")
                            it.transform { it["percentile"].asDouble() }.isEqualTo(45.0)
                            it.transform { it["value"].asDouble() }.isEqualTo(54.5)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("percentile")
                            it.transform { it["percentile"].asDouble() }.isEqualTo(74.5)
                            it.transform { it["value"].asDouble() }.isEqualTo(548.5)
                        }
                    }
                    transform { it["tags"] as ObjectNode }.all {
                        transform { it.size() }.isEqualTo(5)
                        transform { it["campaign"].asText() }.isEqualTo("fourth CAMPAIGN 283239")
                        transform { it["scenario"].asText() }.isEqualTo("fourth scenario")
                        transform { it["step"].asText() }.isEqualTo("step quatro")
                        transform { it["dist"].asText() }.isEqualTo("summary")
                        transform { it["local"].asText() }.isEqualTo("host")
                    }
                }
            }

            // Verification of the rate values.
            any { hit ->
                hit.transform { it["_source"] as ObjectNode }.all {
                    transform { it.size() }.isEqualTo(5)
                    transform { it["name"].asText() }.isEqualTo("my-rate")
                    transform { it["@type"].asText() }.isEqualTo("rate")
                    transform { it["@timestamp"].asLong() }.isNotNull()
                    transform { it["metrics"] }.all {
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("value")
                            it.transform { it["value"].asInt() }.isEqualTo(2)
                        }
                    }
                    transform { it["tags"] as ObjectNode }.all {
                        transform { it.size() }.isEqualTo(5)
                        transform { it["campaign"].asText() }.isEqualTo("campaign 39")
                        transform { it["scenario"].asText() }.isEqualTo("fifth scenario")
                        transform { it["step"].asText() }.isEqualTo("step number five")
                        transform { it["foo"].asText() }.isEqualTo("bar")
                        transform { it["local"].asText() }.isEqualTo("host")
                    }
                }
            }

            // Verification of the throughput values.
            any { hit ->
                hit.transform { it["_source"] as ObjectNode }.all {
                    transform { it.size() }.isEqualTo(5)
                    transform { it["name"].asText() }.isEqualTo("throughput")
                    transform { it["@type"].asText() }.isEqualTo("throughput")
                    transform { it["@timestamp"].asLong() }.isNotNull()
                    transform { it["metrics"] }.all {
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("value")
                            it.transform { it["value"].asInt() }.isEqualTo(30)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("mean")
                            it.transform { it["value"].asInt() }.isEqualTo(22)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("total")
                            it.transform { it["value"].asDouble() }.isEqualTo(173.0)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("max")
                            it.transform { it["value"].asDouble() }.isEqualTo(42.0)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("percentile")
                            it.transform { it["percentile"].asDouble() }.isEqualTo(85.0)
                            it.transform { it["value"].asDouble() }.isEqualTo(42.0)
                        }
                        any {
                            it.transform { it["statistic"].asText() }.isEqualTo("percentile")
                            it.transform { it["percentile"].asDouble() }.isEqualTo(50.0)
                            it.transform { it["value"].asDouble() }.isEqualTo(30.0)
                        }
                    }
                    transform { it["tags"] as ObjectNode }.all {
                        transform { it.size() }.isEqualTo(5)
                        transform { it["campaign"].asText() }.isEqualTo("CEAD@E28339")
                        transform { it["scenario"].asText() }.isEqualTo("sixth scenario")
                        transform { it["step"].asText() }.isEqualTo("step number six")
                        transform { it["a"].asText() }.isEqualTo("b")
                        transform { it["c"].asText() }.isEqualTo("d")
                    }
                }
            }
        }

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
        assertThat(countTypeHits).hasSize(3)
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

    private fun createData(): List<MeterSnapshot> {
        val now = Instant.now()
        val countSnapshot = mockk<MeterSnapshot> {
            every { timestamp } returns now
            every { meterId } returns Meter.Id(
                "my counter",
                MeterType.COUNTER,
                mapOf(
                    "scenario" to "first scenario",
                    "campaign" to "first campaign 5473653",
                    "step" to "step number one"
                )
            )
            every { measurements } returns listOf(MeasurementMetric(9.0, Statistic.COUNT))
        }
        val gaugeSnapshot = mockk<MeterSnapshot> {
            every { timestamp } returns now
            every { meterId } returns Meter.Id(
                "my gauge",
                MeterType.GAUGE,
                mapOf(
                    "foo" to "bar",
                    "any-tag" to "one",
                    "scenario" to "third scenario",
                    "campaign" to "third CAMPAIGN 7624839",
                    "step" to "step number three"
                )
            )
            every { measurements } returns listOf(MeasurementMetric(5.0, Statistic.VALUE))
        }
        val timerSnapshot = mockk<MeterSnapshot> {
            every { timestamp } returns now
            every { meterId } returns Meter.Id(
                "my timer",
                MeterType.TIMER,
                mapOf(
                    "scenario" to "second scenario",
                    "campaign" to "second campaign 47628233",
                    "step" to "step number two"
                )
            )
            every { measurements } returns listOf(
                MeasurementMetric(80.0, Statistic.COUNT),
                MeasurementMetric(224.0, Statistic.MEAN),
                MeasurementMetric(178713.0, Statistic.TOTAL_TIME),
                MeasurementMetric(54328.5, Statistic.MAX),
                DistributionMeasurementMetric(548.5, Statistic.PERCENTILE, 85.0),
                DistributionMeasurementMetric(54328.5, Statistic.PERCENTILE, 50.0),
            )
        }
        val summarySnapshot = mockk<MeterSnapshot> {
            every { timestamp } returns now
            every { meterId } returns Meter.Id(
                "my final summary",
                MeterType.DISTRIBUTION_SUMMARY,
                mapOf(
                    "dist" to "summary",
                    "local" to "host",
                    "scenario" to "fourth scenario",
                    "campaign" to "fourth CAMPAIGN 283239",
                    "step" to "step quatro"
                )
            )
            every { measurements } returns listOf(
                MeasurementMetric(70.0, Statistic.COUNT),
                MeasurementMetric(22.0, Statistic.MEAN),
                MeasurementMetric(17873213.0, Statistic.TOTAL),
                MeasurementMetric(548.5, Statistic.MAX),
                DistributionMeasurementMetric(54.5, Statistic.PERCENTILE, 45.0),
                DistributionMeasurementMetric(548.5, Statistic.PERCENTILE, 74.5),
            )
        }
        val rateSnapshot = mockk<MeterSnapshot> {
            every { timestamp } returns now
            every { meterId } returns Meter.Id(
                "my Rate",
                MeterType.RATE,
                mapOf(
                    "scenario" to "fifth scenario",
                    "campaign" to "campaign 39",
                    "step" to "step number five",
                    "foo" to "bar",
                    "local" to "host"
                )
            )
            every { measurements } returns listOf(
                MeasurementMetric(2.0, Statistic.VALUE)
            )
        }
        val throughputSnapshot = mockk<MeterSnapshot> {
            every { timestamp } returns now
            every { meterId } returns Meter.Id(
                "throughput",
                MeterType.THROUGHPUT,
                mapOf(
                    "scenario" to "sixth scenario",
                    "campaign" to "CEAD@E28339",
                    "step" to "step number six",
                    "a" to "b",
                    "c" to "d"
                )
            )
            every { measurements } returns listOf(
                MeasurementMetric(30.0, Statistic.VALUE),
                MeasurementMetric(22.0, Statistic.MEAN),
                MeasurementMetric(173.0, Statistic.TOTAL),
                MeasurementMetric(42.0, Statistic.MAX),
                DistributionMeasurementMetric(42.0, Statistic.PERCENTILE, 85.0),
                DistributionMeasurementMetric(30.0, Statistic.PERCENTILE, 50.0),
            )
        }
        return listOf(countSnapshot, gaugeSnapshot, timerSnapshot, summarySnapshot, rateSnapshot, throughputSnapshot)
    }

    private fun requestEvents(): ObjectNode {
        restClient.performRequest(Request("POST", "/_refresh"))
        val searchRequest = Request("GET", "/qalipsis-meters/_search?size=100")
        val response = EntityUtils.toString(restClient.performRequest(searchRequest).entity)
        return ObjectMapper().readTree(response)["hits"] as ObjectNode
    }
}
