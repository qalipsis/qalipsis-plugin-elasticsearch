package io.qalipsis.plugins.elasticsearch.save

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isTrue
import assertk.assertions.prop
import com.fasterxml.jackson.databind.json.JsonMapper
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.elasticsearch
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.test.runBlockingTest
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Test

/**
 *
 * @author Alex Averyanov
 */
class ElasticsearchSaveStepSpecificationImplTest {
    private val documentsFactory: suspend ( (ctx: StepContext<*, *>, input: Any) -> List<Document>) = { _, _ ->
        listOf(
            Document("key1", "_doc", "val1", "json"),
            Document("key3", "_doc", "val3", "json"),
            Document("key3-1", "_doc", "val3-1", "json")
        )
    }
    @Test
    fun `should add minimal configuration for the step`() = runBlockingTest {
        val clientBuilder: () -> RestClient = { RestClient.builder(HttpHost("not-localhost", 10000, "http")).build() }
        val mapperConfigurer: (JsonMapper) -> Unit = relaxedMockk()
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().send {
            name = "my-save-step"
            client(clientBuilder)
            mapper(mapperConfigurer)
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
    fun `should add a configuration for the step with monitoring`() = runBlockingTest {
        val clientBuilder: () -> RestClient = { RestClient.builder(HttpHost("not-localhost", 10000, "http")).build() }
        val mapperConfigurer: (JsonMapper) -> Unit = relaxedMockk()
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().send {
            name = "my-save-step"
            client(clientBuilder)
            mapper(mapperConfigurer)
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
    fun `should add a configuration for the step with logger`() = runBlockingTest {
        val clientBuilder: () -> RestClient = { RestClient.builder(HttpHost("not-localhost", 10000, "http")).build() }
        val mapperConfigurer: (JsonMapper) -> Unit = relaxedMockk()
        val previousStep = DummyStepSpecification()
        previousStep.elasticsearch().send {
            name = "my-save-step"
            client(clientBuilder)
            mapper(mapperConfigurer)
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