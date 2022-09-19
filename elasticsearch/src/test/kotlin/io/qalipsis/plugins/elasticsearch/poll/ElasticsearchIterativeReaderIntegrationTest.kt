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

package io.qalipsis.plugins.elasticsearch.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSameSizeAs
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.prop
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.impl.annotations.RelaxedMockK
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.plugins.elasticsearch.AbstractElasticsearchIntegrationTest
import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_6_IMAGE
import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_7_IMAGE
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.io.readResource
import io.qalipsis.test.io.readResourceLines
import io.qalipsis.test.mockk.WithMockk
import kotlinx.coroutines.channels.Channel
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.RegisterExtension
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.stream.Stream
import kotlin.coroutines.CoroutineContext
import kotlin.math.pow

/**
 *
 * @author Eric Jessé
 */
@Testcontainers
@WithMockk
@Timeout(3, unit = TimeUnit.MINUTES)
internal class ElasticsearchIterativeReaderIntegrationTest : AbstractElasticsearchIntegrationTest() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val records =
        readResourceLines("events-data.csv").map { it.split(",") }.map { Event(Instant.parse(it[0]), it[1], it[2]) }

    @RelaxedMockK
    private lateinit var meterRegistry: MeterRegistry

    @RelaxedMockK
    private lateinit var eventsLogger: EventsLogger

    @RelaxedMockK
    private lateinit var stepStartStopContext: StepStartStopContext

    /**
     * This tests imports all the data in the table in subsequent batches, but filter the values with a WHERE clause
     * in the query to remove the ones for Truck #1.
     */
    @ParameterizedTest(name = "should read all the content, poll after poll without monitoring (ES {0})")
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should read all the content, poll after poll without monitoring`(versionAndPort: ContainerVersionAndPort) =
        testDispatcherProvider.run {
            // given
            val firstBatch = records.subList(0, 11)
            val secondBatch = records.subList(11, 26)
            val thirdBatch = records.subList(26, 39)

            val query = """
                {
                    "query": {
                        "wildcard": {
                            "device": "Car*"
                        }
                    },
                    "sort":["timestamp","device"]
                } """.trimIndent()
            val reader = ElasticsearchIterativeReader(
                ioCoroutineScope = this,
                ioCoroutineContext = testDispatcherProvider.io() as CoroutineContext,
                restClientBuilder = {
                    RestClient.builder(HttpHost("localhost", versionAndPort.port, "http")).build()
                },
                index = "events",
                queryParams = mapOf("ignore_unavailable" to "true"),
                elasticsearchPollStatement = ElasticsearchPollStatementImpl {
                    jsonMapper.readTree(query) as ObjectNode
                },
                jsonMapper = jsonMapper,
                pollDelay = Duration.ofMillis(POLL_TIMEOUT),
                resultsChannelFactory = { Channel(5) },
                meterRegistry = meterRegistry,
                eventsLogger = eventsLogger
            )
            reader.init()
            `populate, read and assert`(
                versionAndPort.port,
                versionAndPort.version,
                reader,
                firstBatch,
                secondBatch,
                thirdBatch,
                "events"
            )
        }

    /**
     * This test generates an exception when there is an error while polling data.
     */
    @ParameterizedTest(name = "should generate an exception when the query is wrong")
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should generate an exception when the query is wrong`(versionAndPort: ContainerVersionAndPort) =
        testDispatcherProvider.run {
            // given
            val firstBatch = records.subList(0, 11)
            val secondBatch = records.subList(11, 26)
            val thirdBatch = records.subList(26, 39)
            val client = RestClient.builder(HttpHost("localhost", versionAndPort.port, "http")).build()
            val query = """
                {
                    "quer": {
                        "wildcard": {
                            "device": "Car"
                        }
                    },
                    "sort": ["timestamp","device"]
                } """.trimIndent()
            val reader = ElasticsearchIterativeReader(
                ioCoroutineScope = this,
                ioCoroutineContext = testDispatcherProvider.io() as CoroutineContext,
                restClientBuilder = { client },
                index = "events-2",
                queryParams = emptyMap(),
                elasticsearchPollStatement = ElasticsearchPollStatementImpl {
                    jsonMapper.readTree(query) as ObjectNode
                },
                jsonMapper = jsonMapper,
                pollDelay = Duration.ofMillis(POLL_TIMEOUT),
                resultsChannelFactory = { Channel(5) },
                meterRegistry = meterRegistry,
                eventsLogger = eventsLogger
            )
            reader.init()

            // when
            val errorMessage = assertThrows<ElasticsearchException> {
                `populate, read and assert`(
                    versionAndPort.port,
                    versionAndPort.version,
                    reader,
                    firstBatch,
                    secondBatch,
                    thirdBatch,
                    "events-2"
                )
            }.message

            // then
            assertThat(errorMessage).isNotNull()
        }


    /**
     * This test generates an exception when there is an error while polling data.
     */
    @ParameterizedTest(name = "should generate an exception when the index does not exist")
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should generate an exception when the index does not exist`(versionAndPort: ContainerVersionAndPort) =
        testDispatcherProvider.run {
            // given
            val firstBatch = records.subList(0, 11)
            val secondBatch = records.subList(11, 26)
            val thirdBatch = records.subList(26, 39)

            val client = RestClient.builder(HttpHost("localhost", versionAndPort.port, "http")).build()
            val query = """ {
                    "query": {
                        "wildcard": {
                            "device": "Car*"
                        }
                    },
                    "sort": ["timestamp","device"]
                } """.trimIndent()
            val reader = ElasticsearchIterativeReader(
                ioCoroutineScope = this,
                ioCoroutineContext = testDispatcherProvider.io() as CoroutineContext,
                restClientBuilder = { client },
                index = "event-3",
                queryParams = emptyMap(),
                elasticsearchPollStatement = ElasticsearchPollStatementImpl {
                    jsonMapper.readTree(query) as ObjectNode
                },
                jsonMapper = jsonMapper,
                pollDelay = Duration.ofMillis(POLL_TIMEOUT),
                resultsChannelFactory = { Channel(5) },
                meterRegistry = meterRegistry,
                eventsLogger = eventsLogger
            )

            // when
            val errorMessage = assertThrows<ElasticsearchException> {
                `populate, read and assert`(
                    versionAndPort.port,
                    versionAndPort.version,
                    reader,
                    firstBatch,
                    secondBatch,
                    thirdBatch,
                    "not-existing-index"
                )
            }.message

            // then
            assertThat(errorMessage).isNotNull()
        }

    /**
     * Populates the table batch by batch, and verifies the fetched data at each stage.
     *
     * Since the delivery strategy is "at least once", the bound records of the batches are repeated in the next poll.
     */
    private suspend fun `populate, read and assert`(
        port: Int,
        version: Int,
        reader: ElasticsearchIterativeReader,
        firstBatch: List<Event>,
        secondBatch: List<Event>,
        thirdBatch: List<Event>,
        index: String
    ) {
        val client = RestClient.builder(HttpHost("localhost", port, "http")).build()
        createIndex(client, index, readResource("events-mapping-$version.json"))

        // when
        // Executes a first poll to verify that no empty set is provided.
        reader.start(stepStartStopContext)
        reader.coInvokeInvisible<Unit>("poll", client)

        bulk(client, index, firstBatch.map { DocumentWithId("${UUID.randomUUID()}", it.json) }, version < 7)
        assertThat(count(client, index)).isEqualTo(firstBatch.size)
        reader.coInvokeInvisible<Unit>("poll", client)

        bulk(client, index, secondBatch.map { DocumentWithId("${UUID.randomUUID()}", it.json) }, version < 7)
        assertThat(count(client, index)).isEqualTo(firstBatch.size + secondBatch.size)
        reader.coInvokeInvisible<Unit>("poll", client)

        bulk(client, index, thirdBatch.map { DocumentWithId("${UUID.randomUUID()}", it.json) }, version < 7)
        assertThat(count(client, index)).isEqualTo(firstBatch.size + secondBatch.size + thirdBatch.size)
        reader.coInvokeInvisible<Unit>("poll", client)

        // then
        val firstFetchedBatch = reader.next()
        val secondFetchedBatch = reader.next()
        val thirdFetchedBatch = reader.next()

        assertEqualsForCarsOnly(firstFetchedBatch, firstBatch)
        assertEqualsForCarsOnly(secondFetchedBatch, secondBatch)
        assertEqualsForCarsOnly(thirdFetchedBatch, thirdBatch)
    }

    private fun assertEqualsForCarsOnly(fetched: List<ObjectNode>, inserted: List<Event>) {
        val expectedForCarsOnly = inserted.filterNot { it.device == "Truck #1" }
        assertThat(fetched)
            .transform { it.map { r -> jsonMapper.treeToValue(r.get("_source"), Event::class.java) } }.all {
                hasSameSizeAs(expectedForCarsOnly)
                expectedForCarsOnly.forEachIndexed { index, expected ->
                    index(index).all {
                        prop(Event::timestamp).isEqualTo(expected.timestamp)
                        prop(Event::device).isEqualTo(expected.device)
                        prop(Event::eventname).isEqualTo(expected.eventname)
                    }
                }
            }
    }

    data class Event(
        val timestamp: Instant,
        val device: String,
        val eventname: String
    ) {
        val json = """{"timestamp":"$timestamp","device":"$device","eventname":"$eventname"}"""
    }

    data class ContainerVersionAndPort(val version: Int, val port: Int) {
        override fun toString(): String {
            return "$version"
        }
    }

    companion object {

        @Container
        @JvmStatic
        private val es6 =
            ElasticsearchContainer(DockerImageName.parse(ELASTICSEARCH_6_IMAGE)).apply {
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
                withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
                withEnv("action.destructive_requires_name", "false")
            }

        @Container
        @JvmStatic
        private val es7 =
            ElasticsearchContainer(DockerImageName.parse(ELASTICSEARCH_7_IMAGE)).apply {
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
                withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
                withEnv("action.destructive_requires_name", "false")
            }

        private const val POLL_TIMEOUT = 1000L

        @JvmStatic
        fun containers() = Stream.of(
            Arguments.of(ContainerVersionAndPort(6, es6.getMappedPort(9200))),
            Arguments.of(ContainerVersionAndPort(7, es7.getMappedPort(9200)))
        )
    }
}
