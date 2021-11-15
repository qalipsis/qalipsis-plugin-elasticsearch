package io.qalipsis.plugins.elasticsearch.mget

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.fasterxml.jackson.databind.json.JsonMapper
import io.aerisconsulting.catadioptre.getProperty
import io.mockk.confirmVerified
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.plugins.elasticsearch.ElasticsearchSearchMetricsConfiguration
import io.qalipsis.plugins.elasticsearch.elasticsearch
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.test.runBlockingTest
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import kotlin.random.Random

/**
 *
 * @author Eric Jessé
 */
internal class ElasticsearchMultiGetStepSpecificationImplTest {

    @Test
    internal fun `should add minimal specification to the step`() = runBlockingTest {
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
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::metrics).all {
                prop(ElasticsearchSearchMetricsConfiguration::receivedSuccessBytesCount).isFalse()
                prop(ElasticsearchSearchMetricsConfiguration::receivedFailureBytesCount).isFalse()
                prop(ElasticsearchSearchMetricsConfiguration::receivedDocumentsCount).isFalse()
                prop(ElasticsearchSearchMetricsConfiguration::timeToResponse).isFalse()
                prop(ElasticsearchSearchMetricsConfiguration::successCount).isFalse()
                prop(ElasticsearchSearchMetricsConfiguration::failureCount).isFalse()
            }
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
            metrics {
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
            prop(ElasticsearchMultiGetStepSpecificationImpl<*>::metrics).all {
                prop(ElasticsearchSearchMetricsConfiguration::receivedSuccessBytesCount).isTrue()
                prop(ElasticsearchSearchMetricsConfiguration::receivedFailureBytesCount).isTrue()
                prop(ElasticsearchSearchMetricsConfiguration::receivedDocumentsCount).isTrue()
                prop(ElasticsearchSearchMetricsConfiguration::timeToResponse).isTrue()
                prop(ElasticsearchSearchMetricsConfiguration::successCount).isTrue()
                prop(ElasticsearchSearchMetricsConfiguration::failureCount).isTrue()
            }
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
