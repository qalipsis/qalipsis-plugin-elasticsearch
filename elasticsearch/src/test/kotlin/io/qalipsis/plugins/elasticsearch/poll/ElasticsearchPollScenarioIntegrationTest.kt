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
