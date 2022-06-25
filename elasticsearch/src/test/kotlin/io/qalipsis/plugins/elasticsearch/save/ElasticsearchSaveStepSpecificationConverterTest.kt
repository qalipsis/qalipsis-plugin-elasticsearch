package io.qalipsis.plugins.elasticsearch.save

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.CoroutineScope
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 *
 * @author Alex Averyanov
 */
@WithMockk
internal class ElasticsearchSaveStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<ElasticsearchSaveStepSpecificationConverter>() {

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

    private val restClientBuilder: () -> RestClient = { relaxedMockk() }

    @RelaxedMockK
    private lateinit var ioCoroutineScope: CoroutineScope

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk()))
            .isFalse()
    }

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<ElasticsearchSaveStepSpecificationImpl<*>>()))
            .isTrue()
    }

    @Test
    fun `should convert with name, retry policy and meters`() = testDispatcherProvider.runTest {
        // given
        val spec = ElasticsearchSaveStepSpecificationImpl<Any>()
        spec.also {
            it.name = "my-step"
            it.client = restClientBuilder
            it.documentsFactory = documentsFactory
            it.retryPolicy = mockedRetryPolicy
            it.monitoring {
                meters = true
                events = false
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter)
        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<ElasticsearchSaveStepSpecificationImpl<*>>
        )

        // then
        assertThat(creationContext.createdStep!!).all {
            prop("name").isNotNull().isEqualTo("my-step")
            prop("elasticsearchSaveQueryClient").all {
                prop("clientBuilder").isNotNull().isSameAs(restClientBuilder)
                prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                prop("meterRegistry").isNotNull().isSameAs(meterRegistry)
                prop("eventsLogger").isNull()
            }
            prop("retryPolicy").isNotNull()
            prop("documentsFactory").isEqualTo(documentsFactory)
        }
    }

    @Test
    fun `should convert without name and retry policy but with events`() = testDispatcherProvider.runTest {
        // given
        val spec = ElasticsearchSaveStepSpecificationImpl<Any>()
        spec.also {
            it.name = "my-step"
            it.client = restClientBuilder
            it.documentsFactory = documentsFactory
            it.retryPolicy = mockedRetryPolicy
            it.monitoring {
                meters = false
                events = true
            }
        }

        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<ElasticsearchSaveStepSpecificationImpl<*>>
        )

        // then
        assertThat(creationContext.createdStep!!).all {
            prop("name").isNotNull().isEqualTo("my-step")
            prop("elasticsearchSaveQueryClient").all {
                prop("clientBuilder").isNotNull().isSameAs(restClientBuilder)
                prop("meterRegistry").isNull()
                prop("eventsLogger").isNotNull().isSameAs(eventsLogger)
            }
            prop("retryPolicy").isNotNull()
            prop("documentsFactory").isEqualTo(documentsFactory)
        }
    }

}