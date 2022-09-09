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

package io.qalipsis.plugins.elasticsearch.search

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.fasterxml.jackson.databind.json.JsonMapper
import io.aerisconsulting.catadioptre.getProperty
import io.mockk.confirmVerified
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.plugins.elasticsearch.elasticsearch
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import kotlin.random.Random

/**
 * @author Eric Jessé
 */
internal class ElasticsearchSearchStepSpecificationImplTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    internal fun `should add minimal specification to the step`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().search { }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchSearchStepSpecificationImpl::class).all {
            prop(ElasticsearchSearchStepSpecificationImpl<*>::name).isEmpty()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::client).isNotNull()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::mapper).isNotNull()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::queryFactory).isNotNull()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::paramsFactory).isNotNull()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::indicesFactory).isNotNull()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::convertFullDocument).isFalse()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::targetClass).isEqualTo(Map::class)
            prop(ElasticsearchSearchStepSpecificationImpl<*>::fetchAll).isFalse()
        }
        val mapperConfigurer = previousStep.nextSteps[0].getProperty<(JsonMapper) -> Unit>("mapper")
        val jsonMapper = relaxedMockk<JsonMapper>()
        mapperConfigurer(jsonMapper)
        confirmVerified(jsonMapper)

        val indicesFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<String>>(
                "indicesFactory")
        assertThat(indicesFactory(relaxedMockk(), relaxedMockk())).all {
            hasSize(1)
            containsExactly("_all")
        }

        val queryFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>(
                "queryFactory")
        assertThat(queryFactory(relaxedMockk(), relaxedMockk())).isEqualTo(
            """{"query":{"match_all":{}},"sort":"_id"}""")

        val paramsFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> Map<String, String?>>(
                "paramsFactory")
        assertThat(paramsFactory(relaxedMockk(), relaxedMockk())).hasSize(0)
    }

    @Test
    internal fun `should add a complete specification to the step`() {
        val clientBuilder: () -> RestClient = relaxedMockk()
        val mapperConfigurer: (JsonMapper) -> Unit = relaxedMockk()
        val indicesFactory: suspend (ctx: StepContext<*, *>, input: Int) -> List<String> = relaxedMockk()
        val queryFactory: suspend (ctx: StepContext<*, *>, input: Int) -> String = relaxedMockk()
        val paramsFactory: suspend (ctx: StepContext<*, *>, input: Int) -> Map<String, String?> = relaxedMockk()
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().search {
            name = "my-step"
            client(clientBuilder)
            mapper(mapperConfigurer)
            index(indicesFactory)
            query(queryFactory)
            queryParameters(paramsFactory)
            monitoring {
                all()
            }
            fetchAll()
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchSearchStepSpecificationImpl::class).all {
            prop(ElasticsearchSearchStepSpecificationImpl<*>::name).isEqualTo("my-step")
            prop(ElasticsearchSearchStepSpecificationImpl<*>::client).isSameAs(clientBuilder)
            prop(ElasticsearchSearchStepSpecificationImpl<*>::mapper).isSameAs(mapperConfigurer)
            prop(ElasticsearchSearchStepSpecificationImpl<*>::queryFactory).isSameAs(queryFactory)
            prop(ElasticsearchSearchStepSpecificationImpl<*>::paramsFactory).isSameAs(paramsFactory)
            prop(ElasticsearchSearchStepSpecificationImpl<*>::indicesFactory).isSameAs(indicesFactory)
            prop(ElasticsearchSearchStepSpecificationImpl<*>::convertFullDocument).isFalse()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::targetClass).isEqualTo(Map::class)
            prop(ElasticsearchSearchStepSpecificationImpl<*>::fetchAll).isTrue()
        }
    }

    @Test
    internal fun `should deserialize the source only to the expected class`() {
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().search {}.deserialize(Random::class)

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchSearchStepSpecificationImpl::class).all {
            prop(ElasticsearchSearchStepSpecificationImpl<*>::convertFullDocument).isFalse()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::targetClass).isEqualTo(Random::class)
        }
    }

    @Test
    internal fun `should deserialize the full document`() {
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().search {}.deserialize(Random::class, true)

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchSearchStepSpecificationImpl::class).all {
            prop(ElasticsearchSearchStepSpecificationImpl<*>::convertFullDocument).isTrue()
            prop(ElasticsearchSearchStepSpecificationImpl<*>::targetClass).isEqualTo(Random::class)
        }
    }
}
