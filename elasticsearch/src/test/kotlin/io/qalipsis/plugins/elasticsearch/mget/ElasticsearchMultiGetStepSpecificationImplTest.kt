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

package io.qalipsis.plugins.elasticsearch.mget

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
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import kotlin.random.Random

/**
 *
 * @author Eric Jessé
 */
internal class ElasticsearchMultiGetStepSpecificationImplTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    internal fun `should add minimal specification to the step`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().mget { }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchMultiGetStepSpecificationImpl::class).all {
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::name).isEmpty()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::client).isNotNull()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::mapper).isNotNull()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::queryFactory).isNotNull()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::paramsFactory).isNotNull()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::convertFullDocument).isFalse()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::targetClass).isEqualTo(Map::class)
        }
        val mapperConfigurer = previousStep.nextSteps[0].getProperty<(JsonMapper) -> Unit>("mapper")
        val jsonMapper = relaxedMockk<JsonMapper>()
        mapperConfigurer(jsonMapper)
        confirmVerified(jsonMapper)

        val paramsFactory =
            previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> Map<String, String?>>(
                "paramsFactory")
        assertThat(paramsFactory(relaxedMockk(), relaxedMockk())).hasSize(0)
    }

    @Test
    internal fun `should add a complete specification to the step`() {
        val clientFactory: () -> RestClient = relaxedMockk()
        val mapperConfigurer: (JsonMapper) -> Unit = relaxedMockk()
        val queryFactory: suspend MultiGetQueryBuilder.(ctx: StepContext<*, *>, input: Int) -> Unit = relaxedMockk()
        val paramsFactory: suspend (ctx: StepContext<*, *>, input: Int) -> Map<String, String?> = relaxedMockk()
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().mget {
            name = "my-step"
            client(clientFactory)
            mapper(mapperConfigurer)
            query(queryFactory)
            queryParameters(paramsFactory)
            monitoring {
                all()
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchMultiGetStepSpecificationImpl::class).all {
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::name).isEqualTo("my-step")
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::client).isSameAs(clientFactory)
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::mapper).isSameAs(mapperConfigurer)
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::queryFactory).isSameAs(queryFactory)
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::paramsFactory).isSameAs(paramsFactory)
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::convertFullDocument).isFalse()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::targetClass).isEqualTo(Map::class)
        }
    }

    @Test
    internal fun `should deserialize the source only to the expected class`() {
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().mget {}.deserialize(Random::class)

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchMultiGetStepSpecificationImpl::class).all {
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::convertFullDocument).isFalse()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::targetClass).isEqualTo(Random::class)
        }
    }

    @Test
    internal fun `should deserialize the full document`() {
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().mget {}.deserialize(Random::class, true)

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchMultiGetStepSpecificationImpl::class).all {
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::convertFullDocument).isTrue()
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::targetClass).isEqualTo(Random::class)
        }
    }
}
