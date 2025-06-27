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

package io.qalipsis.plugins.elasticsearch.query

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.plugins.elasticsearch.AbstractElasticsearchIntegrationTest
import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_6_IMAGE
import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_7_IMAGE
import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_8_IMAGE
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.plugins.elasticsearch.query.model.ElasticsearchDocumentsQueryMetrics
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.io.readResource
import io.qalipsis.test.io.readResourceLines
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
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
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Stream
import kotlin.math.pow

/**
 *
 * @author Eric Jessé
 */
@Testcontainers
@WithMockk
@Timeout(3, unit = TimeUnit.MINUTES)
internal class ElasticsearchDocumentsQueryClientImplIntegrationTest : AbstractElasticsearchIntegrationTest() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val eventsLogger = relaxedMockk<EventsLogger>()

    private val elasticsearchDocumentsQueryMetrics = relaxedMockk<ElasticsearchDocumentsQueryMetrics>()

    private val eventTags = relaxedMockk<Map<String, String>>()

    private var initialized = false

    private val records =
        readResourceLines("events-data.csv").map { it.split(",") }.map { Event(Instant.parse(it[0]), it[1], it[2]) }

    private val clientsByVersion = mutableMapOf<Int, RestClient>()

    @BeforeEach
    internal fun setUp() {
        if (!initialized) {
            containers()
                // Map to version and port.
                .map {
                    val versionAndPort = it.get()[0] as ContainerVersionAndPort
                    val client = RestClient.builder(HttpHost("localhost", versionAndPort.port, "http")).build()
                    clientsByVersion[versionAndPort.version] = client

                    versionAndPort.version to client
                }
                .forEach {
                    val version = it.first
                    val client = it.second
                    val versionNumber = if (version >= 7) "7+" else "6"
                    createIndex(client, "events", readResource("events-mapping-$versionNumber.json"))
                    bulk(client, "events", records.map { DocumentWithId(it.id, it.json) }, version < 7)
                }

            clients.set(clientsByVersion.values)
            initialized = true
        }
    }

    @ParameterizedTest(name = "should fetch all the documents for the cars (ES {0})")
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should fetch all the documents for the cars`(versionAndPort: ContainerVersionAndPort) =
        testDispatcherProvider.run {
            // given
            val client = clientsByVersion[versionAndPort.version]!!

            @Suppress("UNCHECKED_CAST")
            val queryClient = ElasticsearchDocumentsQueryClientImpl(
                ioCoroutineContext = this.coroutineContext,
                endpoint = "_search",
                jsonMapper = jsonMapper,
                documentsExtractor = { (it.get("hits")?.get("hits") as ArrayNode).toList() as List<ObjectNode> },
                converter = { jsonMapper.treeToValue(it.get("_source"), Event::class.java) }
            )

            // when
            val results = queryClient.execute(
                client,
                listOf("events"),
                """{"query":{"bool":{"must":[{"match_all":{}}],"filter":[{"wildcard":{"device":"Car*"}}]}}}""",
                mapOf("size" to "100"),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags
            )

            // then
            assertThat(results).all {
                prop(SearchResult<Event>::isSuccess).isTrue()
                prop(SearchResult<Event>::totalResults).isEqualTo(26)
                prop(SearchResult<Event>::results).all {
                    hasSize(26)
                    each { it.prop(ElasticsearchDocument<Event>::value).prop(Event::device).startsWith("Car ") }
                }
                prop(SearchResult<Event>::scrollId).isNull()
                prop(SearchResult<Event>::searchAfterTieBreaker).isNull()
            }
        }

    @ParameterizedTest(name = "should throw an exception when there is a failure")
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should throw an exception when there is a failure fetching documents for the cars`(versionAndPort: ContainerVersionAndPort) =
        testDispatcherProvider.run {
            // given
            val client = clientsByVersion[versionAndPort.version]!!

            @Suppress("UNCHECKED_CAST")
            val queryClient = ElasticsearchDocumentsQueryClientImpl(
                ioCoroutineContext = this.coroutineContext,
                endpoint = "_search",
                jsonMapper = jsonMapper,
                documentsExtractor = { (it.get("hits")?.get("hits") as ArrayNode).toList() as List<ObjectNode> },
                converter = { jsonMapper.treeToValue(it.get("_source"), Event::class.java) }
            )

            // when
            val errorMessage = assertThrows<ElasticsearchException> {
                queryClient.execute(
                    client,
                    listOf("events"),
                    """{"quer":{"bool":{"must":[{"match_all":{}}],"filter":[{"wildcard":{"device":"Car"}}]}}}""",
                    mapOf("size" to "100"),
                    elasticsearchDocumentsQueryMetrics,
                    eventsLogger,
                    eventTags
                )
            }.message


            // then
            assertThat(errorMessage).isNotNull()
        }


    @ParameterizedTest(
        name = "should fetch the first page of the documents for the cars and return the cursor when a scroll time is set (ES {0})"
    )
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should fetch the first page of the documents for the cars and return the cursor when a scroll time is set`(
        versionAndPort: ContainerVersionAndPort
    ) = testDispatcherProvider.run {
        // given
        val client = clientsByVersion[versionAndPort.version]!!

        @Suppress("UNCHECKED_CAST")
        val queryClient = ElasticsearchDocumentsQueryClientImpl(
            ioCoroutineContext = this.coroutineContext,
            endpoint = "_search",
            jsonMapper = jsonMapper,
            documentsExtractor = { (it.get("hits")?.get("hits") as ArrayNode).toList() as List<ObjectNode> },
            converter = { jsonMapper.treeToValue(it.get("_source"), Event::class.java) }
        )

        // when
        val results = queryClient.execute(
            client,
            listOf("events"),
            """{"query":{"bool":{"must":[{"match_all":{}}],"filter":[{"wildcard":{"device":"Car*"}}]}}}""",
            mapOf("scroll" to "10s", "size" to "10"),
            elasticsearchDocumentsQueryMetrics,
            eventsLogger,
            eventTags
        )

        // then
        assertThat(results).all {
            prop(SearchResult<Event>::isSuccess).isTrue()
            prop(SearchResult<Event>::totalResults).isEqualTo(26)
            prop(SearchResult<Event>::results).hasSize(10)
            prop(SearchResult<Event>::scrollId).isNotNull()
            prop(SearchResult<Event>::searchAfterTieBreaker).isNull()
        }

        val scrollResults = queryClient.scroll(
            client, "10s", results.scrollId!!,
            elasticsearchDocumentsQueryMetrics,
            eventsLogger,
            eventTags
        )

        // then
        assertThat(scrollResults).all {
            prop(SearchResult<Event>::isSuccess).isTrue()
            prop(SearchResult<Event>::totalResults).isEqualTo(26)
            prop(SearchResult<Event>::results).hasSize(10)
            prop(SearchResult<Event>::scrollId).isNotNull()
            prop(SearchResult<Event>::searchAfterTieBreaker).isNull()
        }

        queryClient.clearScroll(client, results.scrollId!!)
    }

    @ParameterizedTest(
        name = "should fetch the first page of the documents for the cars and return the search after when a sort is set (ES {0})"
    )
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should fetch the first page of the documents for the cars and return the search after when a sort is set`(
        versionAndPort: ContainerVersionAndPort
    ) = testDispatcherProvider.run {
        // given
        val client = clientsByVersion[versionAndPort.version]!!

        @Suppress("UNCHECKED_CAST")
        val queryClient = ElasticsearchDocumentsQueryClientImpl(
            ioCoroutineContext = this.coroutineContext,
            endpoint = "_search",
            jsonMapper = jsonMapper,
            documentsExtractor = { (it.get("hits")?.get("hits") as ArrayNode).toList() as List<ObjectNode> },
            converter = { jsonMapper.treeToValue(it.get("_source"), Event::class.java) }

        )

        // when
        val results = queryClient.execute(
            client,
            listOf("events"),
            """{"query":{"bool":{"must":[{"match_all":{}}],"filter":[{"wildcard":{"device":"Car*"}}]}},"sort":["timestamp","device"]}""",
            mapOf("size" to "10"),
            elasticsearchDocumentsQueryMetrics,
            eventsLogger,
            eventTags
        )

        // then
        assertThat(results).all {
            prop(SearchResult<Event>::isSuccess).isTrue()
            prop(SearchResult<Event>::totalResults).isEqualTo(26)
            prop(SearchResult<Event>::results).hasSize(10)
            prop(SearchResult<Event>::scrollId).isNull()
            prop(SearchResult<Event>::searchAfterTieBreaker).isNotNull().transform { it.toList() }.all {
                hasSize(2)
                index(0).transform { it.longValue() }.isEqualTo(1603197614000L)
                index(1).transform { it.textValue() }.isEqualTo("Car #2")
            }
        }
    }

    @ParameterizedTest(name = "should perform a multi get (ES {0})")
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should perform a multi get`(versionAndPort: ContainerVersionAndPort) = testDispatcherProvider.run {
        // given
        val client = clientsByVersion[versionAndPort.version]!!

        @Suppress("UNCHECKED_CAST")
        val queryClient = ElasticsearchDocumentsQueryClientImpl(
            ioCoroutineContext = this.coroutineContext,
            endpoint = "_mget",
            jsonMapper = jsonMapper,
            documentsExtractor = {
                (it.get("docs") as ArrayNode).toList()
                    .filter { it.get("found").booleanValue() } as List<ObjectNode>
            },
            converter = { jsonMapper.treeToValue(it.get("_source"), Event::class.java) }
        )
        val record1 = records[5]
        val record2 = records[11]
        val record3 = records[15]

        queryClient.init(client)

        // when
        val results = queryClient.execute(
            client,
            listOf(),
            """{"docs":[{"_index":"events","_type":"_doc","_id":"${record1.id}"},{"_index":"events","_type":"_doc","_id":"${record2.id}"},{"_index":"events","_type":"_doc","_id":"${record3.id}"},{"_index":"events","_type":"_doc","_id":"does_not_exists"}]}""",
            emptyMap(),
            elasticsearchDocumentsQueryMetrics,
            eventsLogger,
            eventTags
        )

        // then
        assertThat(results).all {
            prop(SearchResult<Event>::isSuccess).isTrue()
            prop(SearchResult<Event>::totalResults).isEqualTo(3)
            prop(SearchResult<Event>::results).all {
                hasSize(3)
                index(0).all {
                    prop(ElasticsearchDocument<Event>::id).isEqualTo(record1.id)
                    prop(ElasticsearchDocument<Event>::value).isNotNull()
                }
                index(1).all {
                    prop(ElasticsearchDocument<Event>::id).isEqualTo(record2.id)
                    prop(ElasticsearchDocument<Event>::value).isNotNull()
                }
                index(2).all {
                    prop(ElasticsearchDocument<Event>::id).isEqualTo(record3.id)
                    prop(ElasticsearchDocument<Event>::value).isNotNull()
                }
            }
            prop(SearchResult<Event>::scrollId).isNull()
            prop(SearchResult<Event>::searchAfterTieBreaker).isNull()
        }
    }

    @ParameterizedTest(name = "should generate a failure when the query is not valid (ES {0})")
    @MethodSource("containers")
    @Timeout(20)
    internal fun `should generate a failure when the query is not valid`(versionAndPort: ContainerVersionAndPort) =
        testDispatcherProvider.run {
            // given
            val client = clientsByVersion[versionAndPort.version]!!

            @Suppress("UNCHECKED_CAST")
            val queryClient = ElasticsearchDocumentsQueryClientImpl(
                ioCoroutineContext = this.coroutineContext,
                endpoint = "_search",
                jsonMapper = jsonMapper,
                documentsExtractor = { (it.get("hits")?.get("hits") as ArrayNode).toList() as List<ObjectNode> },
                converter = { jsonMapper.treeToValue(it.get("_source"), Event::class.java) }
            )

            // when
            val errorMessage = assertThrows<ElasticsearchException> {
                queryClient.execute(
                    client, listOf("unexisting-index"), "", emptyMap(),
                    elasticsearchDocumentsQueryMetrics,
                    eventsLogger,
                    eventTags
                )
            }

            // then
            assertThat(errorMessage).isNotNull()
        }

    data class Event(
        val timestamp: Instant,
        val device: String,
        val eventname: String
    ) {
        val id = "${UUID.randomUUID()}"

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

        @Container
        @JvmStatic
        private val es8 =
            ElasticsearchContainer(DockerImageName.parse(ELASTICSEARCH_8_IMAGE)).apply {
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
                withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
                withEnv("action.destructive_requires_name", "false")
                withEnv("xpack.security.enabled", "false")
            }

        @JvmStatic
        private val clients = AtomicReference<Collection<RestClient>>()

        @JvmStatic
        fun containers(): Stream<Arguments> = Stream.of(
            Arguments.of(ContainerVersionAndPort(6, es6.getMappedPort(9200))),
            Arguments.of(ContainerVersionAndPort(7, es7.getMappedPort(9200))),
            Arguments.of(ContainerVersionAndPort(8, es8.getMappedPort(9200)))
        )

        @AfterAll
        fun closeClients() {
            clients.get().forEach(RestClient::close)
        }
    }
}
