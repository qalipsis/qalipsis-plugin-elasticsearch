package io.qalipsis.plugins.elasticsearch.poll

import io.qalipsis.api.annotations.Scenario
import io.qalipsis.api.lang.concurrentList
import io.qalipsis.api.rampup.regular
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
            rampUp {
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
