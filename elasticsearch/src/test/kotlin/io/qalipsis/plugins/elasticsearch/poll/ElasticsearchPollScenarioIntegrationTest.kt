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
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import io.qalipsis.plugins.elasticsearch.AbstractElasticsearchIntegrationTest
import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_7_IMAGE
import io.qalipsis.runtime.test.QalipsisTestRunner
import io.qalipsis.test.io.readResource
import io.qalipsis.test.io.readResourceLines
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Instant
import java.util.UUID
import kotlin.math.pow

/**
 * Integration test to demo the usage of the poll operator in a scenario.
 *
 * See [PollScenario] for more details.
 *
 * @author Eric Jessé
 */
@Testcontainers
internal class ElasticsearchPollScenarioIntegrationTest : AbstractElasticsearchIntegrationTest() {

    data class Move(
        val timestamp: Instant,
        val action: String,
        val username: String
    ) {
        val json = """{"timestamp":"$timestamp","action":"$action","username":"$username"}"""
    }

    @Test
    @Timeout(30)
    internal fun `should run the poll scenario`() {
        // given
        val esPort = es7.getMappedPort(9200)
        PollScenario.esPort = esPort
        val client = RestClient.builder(HttpHost("localhost", esPort, "http")).build()

        createIndex(client, "building-moves", readResource("building-moves-mapping-7.json"))

        val moves = readResourceLines("building-moves.csv").map { it.split(",") }
            .map { Move(Instant.parse(it[0]), it[1], it[2]) }

        bulk(client, "building-moves", moves.map { DocumentWithId("${UUID.randomUUID()}", it.json) }, false)

        // when
        val exitCode = QalipsisTestRunner.withScenarios("elasticsearch-poll").execute()

        // then
        Assertions.assertEquals(0, exitCode)
        assertThat(PollScenario.receivedMessages).all {
            hasSize(5)
            containsExactlyInAnyOrder(
                "The user alice stayed 48 minute(s) in the building",
                "The user bob stayed 20 minute(s) in the building",
                "The user charles stayed 1 minute(s) in the building",
                "The user david stayed 114 minute(s) in the building",
                "The user erin stayed 70 minute(s) in the building"
            )
        }
    }

    companion object {

        @Container
        @JvmStatic
        private val es7 = ElasticsearchContainer(DockerImageName.parse(ELASTICSEARCH_7_IMAGE)).apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
        }

    }
}
