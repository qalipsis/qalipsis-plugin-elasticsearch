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
import assertk.assertions.containsExactly
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import assertk.assertions.key
import assertk.assertions.prop
import com.fasterxml.jackson.databind.json.JsonMapper
import io.aerisconsulting.catadioptre.getProperty
import io.mockk.confirmVerified
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.scenario.TestScenarioFactory
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.plugins.elasticsearch.elasticsearch
import io.qalipsis.test.mockk.relaxedMockk
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.random.Random


/**
 *
 * @author Eric Jessé
 */
internal class ElasticsearchPollStepSpecificationImplTest {

    @Test
    internal fun `should add minimal specification to the scenario`() {
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.elasticsearch().poll {
            pollDelay(Duration.ofSeconds(12))
        }

        assertThat(scenario.rootSteps[0]).isInstanceOf(ElasticsearchPollStepSpecificationImpl::class).all {
            prop(ElasticsearchPollStepSpecificationImpl::name).isEmpty()
            prop(ElasticsearchPollStepSpecificationImpl::client).isNotNull()
            prop(ElasticsearchPollStepSpecificationImpl::queryFactory).isNotNull()
            prop(ElasticsearchPollStepSpecificationImpl::mapper).isNotNull()
            prop(ElasticsearchPollStepSpecificationImpl::convertFullDocument).isFalse()
            prop(ElasticsearchPollStepSpecificationImpl::flattenOutput).isFalse()
            prop(ElasticsearchPollStepSpecificationImpl::targetClass).isEqualTo(Map::class)
            prop(ElasticsearchPollStepSpecificationImpl::queryParameters).isNotNull().isEmpty()
            prop(ElasticsearchPollStepSpecificationImpl::indices).all {
                hasSize(1)
                containsExactly("_all")
            }
            prop(ElasticsearchPollStepSpecificationImpl::pollDelay).isEqualTo(Duration.ofSeconds(12))
            prop(ElasticsearchPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(-1)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ZERO)
            }
        }

        val queryFactory = scenario.rootSteps[0].getProperty<() -> String>("queryFactory")
        assertThat(queryFactory.invoke()).isEqualTo("""{"query":{"match_all":{}},"sort":"_id"}""")

        val mapperConfigurer = scenario.rootSteps[0].getProperty<(JsonMapper) -> Unit>("mapper")
        val jsonMapper = relaxedMockk<JsonMapper>()
        mapperConfigurer(jsonMapper)
        confirmVerified(jsonMapper)
    }

    @Test
    internal fun `should add a complete specification to the scenario as broadcast`() {
        val clientBuilder: () -> RestClient = { RestClient.builder(HttpHost("not-localhost", 10000, "http")).build() }
        val mapperConfigurer: (JsonMapper) -> Unit = relaxedMockk()
        val query = """{"size":0,"query":{"bool":{"must":[{"match_all":{}}]}},"sort":"timestamp"}"""
        val queryBuilder: () -> String = { query }
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.elasticsearch().poll {
            name = "my-step"
            client(clientBuilder)
            mapper(mapperConfigurer)
            index("index-1", "ind*2")
            queryParameters("param-1" to "val-1", "param-2" to "val-2")
            query(queryBuilder)
            pollDelay(Duration.ofSeconds(12))
            monitoring { all() }
            broadcast(123, Duration.ofSeconds(20))
        }

        assertThat(scenario.rootSteps[0]).isInstanceOf(ElasticsearchPollStepSpecificationImpl::class).all {
            prop(ElasticsearchPollStepSpecificationImpl::name).isEqualTo("my-step")
            prop(ElasticsearchPollStepSpecificationImpl::client).isSameAs(clientBuilder)
            prop(ElasticsearchPollStepSpecificationImpl::queryFactory).isSameAs(queryBuilder)
            prop(ElasticsearchPollStepSpecificationImpl::mapper).isSameAs(mapperConfigurer)
            prop(ElasticsearchPollStepSpecificationImpl::convertFullDocument).isFalse()
            prop(ElasticsearchPollStepSpecificationImpl::flattenOutput).isFalse()
            prop(ElasticsearchPollStepSpecificationImpl::targetClass).isEqualTo(Map::class)
            prop(ElasticsearchPollStepSpecificationImpl::queryParameters).all {
                hasSize(2)
                key("param-1").isEqualTo("val-1")
                key("param-2").isEqualTo("val-2")
            }
            prop(ElasticsearchPollStepSpecificationImpl::indices).all {
                hasSize(2)
                containsExactly("index-1", "ind*2")
            }
            prop(ElasticsearchPollStepSpecificationImpl::pollDelay).isEqualTo(Duration.ofSeconds(12))
            prop(ElasticsearchPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.BROADCAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(123)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofSeconds(20))
            }
        }
    }

    @Test
    internal fun `should deserialize the source only to the expected class`() {
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.elasticsearch().poll {
            pollDelay(Duration.ofSeconds(12))
        }.deserialize(Random::class)

        assertThat(scenario.rootSteps[0]).isInstanceOf(ElasticsearchPollStepSpecificationImpl::class).all {
            prop(ElasticsearchPollStepSpecificationImpl::convertFullDocument).isFalse()
            prop(ElasticsearchPollStepSpecificationImpl::flattenOutput).isFalse()
            prop(ElasticsearchPollStepSpecificationImpl::targetClass).isEqualTo(Random::class)
        }
    }

    @Test
    internal fun `should deserialize the full document`() {
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.elasticsearch().poll {
            pollDelay(Duration.ofSeconds(12))
        }.deserialize(Random::class, true)

        assertThat(scenario.rootSteps[0]).isInstanceOf(ElasticsearchPollStepSpecificationImpl::class).all {
            prop(ElasticsearchPollStepSpecificationImpl::convertFullDocument).isTrue()
            prop(ElasticsearchPollStepSpecificationImpl::flattenOutput).isFalse()
            prop(ElasticsearchPollStepSpecificationImpl::targetClass).isEqualTo(Random::class)
        }
    }

    @Test
    internal fun `should flatten the source only`() {
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.elasticsearch().poll {
            pollDelay(Duration.ofSeconds(12))
        }.flatten(Random::class)

        assertThat(scenario.rootSteps[0]).isInstanceOf(ElasticsearchPollStepSpecificationImpl::class).all {
            prop(ElasticsearchPollStepSpecificationImpl::convertFullDocument).isFalse()
            prop(ElasticsearchPollStepSpecificationImpl::flattenOutput).isTrue()
            prop(ElasticsearchPollStepSpecificationImpl::targetClass).isEqualTo(Random::class)
        }
    }

    @Test
    internal fun `should flatten the full document`() {
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.elasticsearch().poll {
            pollDelay(Duration.ofSeconds(12))
        }.flatten(Random::class, true)

        assertThat(scenario.rootSteps[0]).isInstanceOf(ElasticsearchPollStepSpecificationImpl::class).all {
            prop(ElasticsearchPollStepSpecificationImpl::convertFullDocument).isTrue()
            prop(ElasticsearchPollStepSpecificationImpl::flattenOutput).isTrue()
            prop(ElasticsearchPollStepSpecificationImpl::targetClass).isEqualTo(Random::class)
        }
    }
}
