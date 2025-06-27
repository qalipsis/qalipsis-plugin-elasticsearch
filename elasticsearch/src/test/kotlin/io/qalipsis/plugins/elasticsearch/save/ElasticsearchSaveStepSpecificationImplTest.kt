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

package io.qalipsis.plugins.elasticsearch.save

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isTrue
import assertk.assertions.prop
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.elasticsearch
import io.qalipsis.test.coroutines.TestDispatcherProvider
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 *
 * @author Alex Averyanov
 */
internal class ElasticsearchSaveStepSpecificationImplTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val documentsFactory: suspend ((ctx: StepContext<*, *>, input: Any) -> List<Document>) = { _, _ ->
        listOf(
            Document("key1", "_doc", "val1", "json"),
            Document("key3", "_doc", "val3", "json"),
            Document("key3-1", "_doc", "val3-1", "json")
        )
    }

    @Test
    fun `should add minimal configuration for the step`() = testDispatcherProvider.runTest {
        val clientBuilder: () -> RestClient = { RestClient.builder(HttpHost("not-localhost", 10000, "http")).build() }
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().save {
            name = "my-save-step"
            client(clientBuilder)
            documents(documentsFactory)
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchSaveStepSpecificationImpl::class).all {
            prop("name") { ElasticsearchSaveStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-save-step")
            prop(ElasticsearchSaveStepSpecificationImpl<*>::client).isNotNull()
            prop(ElasticsearchSaveStepSpecificationImpl<*>::documentsFactory).isEqualTo(documentsFactory)
            prop(ElasticsearchSaveStepSpecificationImpl<*>::monitoringConfig).isNotNull().all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
        }

    }


    @Test
    fun `should add a configuration for the step with monitoring`() = testDispatcherProvider.runTest {
        val clientBuilder: () -> RestClient = { RestClient.builder(HttpHost("not-localhost", 10000, "http")).build() }
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().save {
            name = "my-save-step"
            client(clientBuilder)
            documents(documentsFactory)
            monitoring {
                events = false
                meters = true
            }
        }


        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchSaveStepSpecificationImpl::class).all {
            prop("name") { ElasticsearchSaveStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-save-step")
            prop(ElasticsearchSaveStepSpecificationImpl<*>::client).isNotNull()
            prop(ElasticsearchSaveStepSpecificationImpl<*>::documentsFactory).isEqualTo(documentsFactory)
            prop(ElasticsearchSaveStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
        }
    }

    @Test
    fun `should add a configuration for the step with logger`() = testDispatcherProvider.runTest {
        val clientBuilder: () -> RestClient = { RestClient.builder(HttpHost("not-localhost", 10000, "http")).build() }
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().save {
            name = "my-save-step"
            client(clientBuilder)
            documents(documentsFactory)
            monitoring {
                events = true
                meters = false
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(ElasticsearchSaveStepSpecificationImpl::class).all {
            prop("name") { ElasticsearchSaveStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-save-step")
            prop(ElasticsearchSaveStepSpecificationImpl<*>::client).isNotNull()
            prop(ElasticsearchSaveStepSpecificationImpl<*>::documentsFactory).isEqualTo(documentsFactory)
            prop(ElasticsearchSaveStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
        }
    }
}