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

import io.qalipsis.api.annotations.Scenario
import io.qalipsis.api.executionprofile.regular
import io.qalipsis.api.lang.concurrentList
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.filterNotNull
import io.qalipsis.api.steps.innerJoin
import io.qalipsis.api.steps.logErrors
import io.qalipsis.api.steps.map
import io.qalipsis.api.steps.onEach
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.plugins.elasticsearch.elasticsearch
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import java.time.Duration
import java.time.ZonedDateTime

/**
 *
 * Scenario to demo how the poll step can work. The scenario reads the entries in a building on one side and the exits
 * on the other side.
 *
 * Records related to the same person are joined and the duration is then printed out in the console.
 *
 * @author Eric Jessé
 */
object PollScenario {

    private const val minions = 5

    val receivedMessages = concurrentList<String>()

    var esPort: Int = 0

    @Scenario
    fun pollData() {
        scenario("elasticsearch-poll") {
            minionsCount = minions
            profile {
                // Starts all at once.
                regular(100, minionsCount)
            }
        }
            .start()
            .elasticsearch()
            .poll {
                name = "poll.in"
                index("building-moves")
                client {
                    RestClient.builder(HttpHost("localhost", esPort, "http")).build()
                }
                query {
                    """
                     {
                        "query": {
                            "term": {
                                "action": "IN"
                            }
                        },
                        "size": 2,
                        "sort":["timestamp","username"]
                    }
                    """.trimIndent()
                }
                pollDelay(Duration.ofSeconds(1))
            }.flatten(BuildingMove::class)
            .logErrors()
            .map(ElasticsearchDocument<BuildingMove>::value)
            .innerJoin(
                using = { it.value.username },
                on = {
                    it.elasticsearch()
                        .poll {
                            name = "poll.out"
                            index("building-moves")
                            client {
                                RestClient.builder(HttpHost("localhost", esPort, "http")).build()
                            }
                            query {
                                """
                                     {
                                        "query": {
                                            "term": {
                                                "action": "OUT"
                                            }
                                        },
                                        "size": 2,
                                        "sort":["timestamp","username"]
                                    }
                                    """.trimIndent()
                            }
                            pollDelay(Duration.ofSeconds(1))
                        }
                        .flatten(BuildingMove::class)
                        .logErrors()
                        .map(ElasticsearchDocument<BuildingMove>::value)
                },
                having = { it.value.username }
            )
            .filterNotNull()
            .map {
                it.first.username to Duration.between(it.first.timestamp, it.second.timestamp)
            }
            .map { "The user ${it.first} stayed ${it.second.toMinutes()} minute(s) in the building" }
            .onEach { receivedMessages.add(it) }
            .onEach { println(it) }
    }

    data class BuildingMove(val username: String, val action: String, val timestamp: ZonedDateTime)
}
