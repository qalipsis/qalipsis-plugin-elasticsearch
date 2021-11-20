package io.qalipsis.plugins.elasticsearch.mget

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryClientImpl
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryStep
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.test.runBlockingTest
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass

/**
 *
 * @author Eric Jessé
 */
@WithMockk
internal class ElasticsearchMultiGetStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<ElasticsearchMultiGetStepSpecificationConverter>() {

    @RelaxedMockK
    private lateinit var clientBuilder: () -> RestClient

    @RelaxedMockK
    private lateinit var mapperConfigurer: (JsonMapper) -> Unit

    private val multiGetQueryBuilder: (suspend MultiGetQueryBuilder.(ctx: StepContext<*, *>, input: Int) -> Unit) =
        relaxedMockk()

    private val queryFactory: (suspend (ctx: StepContext<*, *>, input: Any?) -> ObjectNode) = relaxedMockk()

    private val paramsFactory: (suspend (ctx: StepContext<*, *>, input: Int) -> Map<String, String?>) = relaxedMockk()

    @RelaxedMockK
    private lateinit var retryPolicy: RetryPolicy

    @RelaxedMockK
    private lateinit var queryClient: ElasticsearchDocumentsQueryClientImpl<Any?>

    @RelaxedMockK
    private lateinit var jsonMapper: JsonMapper

    @RelaxedMockK
    private lateinit var documentExtractor: (JsonNode) -> List<ObjectNode>

    @RelaxedMockK
    private lateinit var documentConverter: (ObjectNode) -> Any?

    @RelaxedMockK
    private lateinit var targetClass: KClass<*>

    @RelaxedMockK
    private lateinit var ioCoroutineContext: CoroutineContext

    @Test
    override fun `should not support unexpected spec`() {
        assertFalse(converter.support(relaxedMockk()))
    }

    @Test
    override fun `should support expected spec`() {
        assertTrue(converter.support(relaxedMockk<ElasticsearchMultiGetStepSpecificationImpl<*>>()))
    }

    @Test
    fun `should convert with name and retry policy`() = runBlockingTest {
        // given
        val spec = ElasticsearchMultiGetStepSpecificationImpl<Int>().also {
            it.name = "my-step"
            it.retry(retryPolicy)
            it.client(clientBuilder)
            it.mapper(mapperConfigurer)
            it.query(multiGetQueryBuilder)
            it.queryParameters(paramsFactory)
        }

        val spiedConverter = spyk(converter)
        every { spiedConverter.buildMapper(refEq(spec)) } returns jsonMapper
        every { spiedConverter.buildQueryFactory(refEq(spec), refEq(jsonMapper)) } returns queryFactory
        every {
            spiedConverter.buildQueryClient(refEq(spec), refEq(jsonMapper))
        } returns queryClient
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        @Suppress("UNCHECKED_CAST")
        spiedConverter.convert<Int, Map<String, Any?>>(
            creationContext as StepCreationContext<ElasticsearchMultiGetStepSpecificationImpl<*>>)

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(ElasticsearchDocumentsQueryStep::class).all {
                prop("id").isEqualTo("my-step")
                prop("retryPolicy").isEqualTo(retryPolicy)
                prop("restClientBuilder").isEqualTo(clientBuilder)
                prop("queryParamsFactory").isEqualTo(paramsFactory)
                prop("queryFactory").isSameAs(queryFactory)
            }
        }
    }

    @Test
    fun `should convert without name nor retry policy`() = runBlockingTest {
        // given
        val spec = ElasticsearchMultiGetStepSpecificationImpl<Int>().also {
            it.client(clientBuilder)
            it.mapper(mapperConfigurer)
            it.query(multiGetQueryBuilder)
            it.queryParameters(paramsFactory)
        }

        val spiedConverter = spyk(converter)
        every { spiedConverter.buildMapper(refEq(spec)) } returns jsonMapper
        every { spiedConverter.buildQueryFactory(refEq(spec), refEq(jsonMapper)) } returns queryFactory
        every {
            spiedConverter.buildQueryClient(refEq(spec), refEq(jsonMapper))
        } returns queryClient
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        @Suppress("UNCHECKED_CAST")
        spiedConverter.convert<Int, Map<String, Any?>>(
            creationContext as StepCreationContext<ElasticsearchMultiGetStepSpecificationImpl<*>>)

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(ElasticsearchDocumentsQueryStep::class).all {
                prop("id").isNotNull()
                prop("retryPolicy").isNull()
                prop("restClientBuilder").isEqualTo(clientBuilder)
                prop("queryParamsFactory").isEqualTo(paramsFactory)
                prop("queryFactory").isSameAs(queryFactory)
            }
        }
    }

    @Test
    internal fun `should build the mapper`() {
        // given
        val spec = ElasticsearchMultiGetStepSpecificationImpl<Int>().also {
            it.mapper(mapperConfigurer)
        }

        // when
        val jsonMapper = converter.buildMapper(spec)

        // then
        verify { mapperConfigurer.invoke(refEq(jsonMapper)) }
    }

    @Test
    internal fun `should build the query client with full document conversion`() {
        // given
        val spec = ElasticsearchMultiGetStepSpecificationImpl<Int>().also {
            it.targetClass = targetClass
            it.convertFullDocument = true
        }
        val spiedConverter = spyk(converter)
        every { spiedConverter.buildDocumentsExtractor(refEq(spec)) } returns documentExtractor
        every {
            spiedConverter.buildConverter(refEq(targetClass), eq(true), refEq(jsonMapper))
        } returns documentConverter

        // when
        val queryClient = spiedConverter.buildQueryClient(spec, jsonMapper)

        // then
        assertThat(queryClient).all {
            prop("endpoint").isEqualTo("_mget")
            prop("jsonMapper").isSameAs(jsonMapper)
            prop("documentsExtractor").isSameAs(documentExtractor)
            prop("converter").isSameAs(documentConverter)
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the query client with conversion of document source only`() {
        // given
        val spec = ElasticsearchMultiGetStepSpecificationImpl<Int>().also {
            it.targetClass = targetClass
            it.convertFullDocument = false
        }
        val spiedConverter = spyk(converter)
        every { spiedConverter.buildDocumentsExtractor(refEq(spec)) } returns documentExtractor
        every {
            spiedConverter.buildConverter(refEq(targetClass), eq(false), refEq(jsonMapper))
        } returns documentConverter

        // when
        val queryClient = spiedConverter.buildQueryClient(spec, jsonMapper)

        // then
        assertThat(queryClient).all {
            prop("endpoint").isEqualTo("_mget")
            prop("jsonMapper").isSameAs(jsonMapper)
            prop("documentsExtractor").isSameAs(documentExtractor)
            prop("converter").isSameAs(documentConverter)
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the documents extractor filtering only the found values with conversion of document source only`() {
        // given
        val jsonMapper = JsonMapper()

        // when
        val documentsExtractor = converter.buildDocumentsExtractor(relaxedMockk {
            every { convertFullDocument } returns false
        })

        // then it should extract results when there are.
        val nodeWithResults = jsonMapper.readTree(
            """{"docs":[{"value":1,"found":true},{"value":2,"found":false},{"value":3,"found":true}]}""")
        assertThat(documentsExtractor(nodeWithResults)).all {
            hasSize(2)
            index(0).transform { it.get("value").intValue() }.isEqualTo(1)
            index(1).transform { it.get("value").intValue() }.isEqualTo(3)
        }

        // then it should return an empty collection when the docs node is missing.
        val emptyNode = jsonMapper.readTree("""{}""")
        assertThat(documentsExtractor(emptyNode)).isEmpty()
    }

    @Test
    internal fun `should build the documents extractor filtering only the found values with conversion of full document`() {
        // given
        val jsonMapper = JsonMapper()

        // when
        val documentsExtractor = converter.buildDocumentsExtractor(relaxedMockk {
            every { convertFullDocument } returns true
        })

        // then it should extract results when there are.
        val nodeWithResults = jsonMapper.readTree(
            """{"docs":[{"value":1,"found":true},{"value":2,"found":false},{"value":3,"found":true}]}""")
        assertThat(documentsExtractor(nodeWithResults)).all {
            hasSize(3)
            index(0).transform { it.get("value").intValue() }.isEqualTo(1)
            index(1).transform { it.get("value").intValue() }.isEqualTo(2)
            index(2).transform { it.get("value").intValue() }.isEqualTo(3)
        }

        // then it should return an empty collection when the docs node is missing.
        val emptyNode = jsonMapper.readTree("""{}""")
        assertThat(documentsExtractor(emptyNode)).isEmpty()
    }

    @Test
    internal fun `should build the query builder`() = runBlockingTest {
        // given
        val jsonMapper = JsonMapper()
        val contextSlot = slot<StepContext<*, *>>()
        val inputSlot = slot<Int>()
        val spec = ElasticsearchMultiGetStepSpecificationImpl<Int>().also {
            it.query { ctx, input ->
                contextSlot.captured = ctx
                inputSlot.captured = input

                doc("index-1", "id-1")
                doc("index-2", "id-2")
            }
        }

        // when
        val queryNodeBuilder = converter.buildQueryFactory(spec, jsonMapper)

        // then
        // Executes the query builder to verify the behavior.
        val stepContext: StepContext<Int, Map<String, Any?>> = relaxedMockk()
        assertThat(queryNodeBuilder(stepContext, 256)).isNotNull().transform { it.toString() }
            .isEqualTo(
                """{"docs":[{"_index":"index-1","_id":"id-1","_source":true},{"_index":"index-2","_id":"id-2","_source":true}]}""")
        assertThat(contextSlot.captured).isSameAs(stepContext)
        assertThat(inputSlot.captured).isEqualTo(256)
    }

    @Test
    fun `should add eventsLogger`() = runBlockingTest {
        // given
        val spec = ElasticsearchMultiGetStepSpecificationImpl<Int>().also {
            it.name = "my-step"
            it.retry(retryPolicy)
            it.client(clientBuilder)
            it.mapper(mapperConfigurer)
            it.query(multiGetQueryBuilder)
            it.queryParameters(paramsFactory)
            it.monitoringConfig = StepMonitoringConfiguration(events = true)
        }

        val spiedConverter = spyk(converter)
        every { spiedConverter.buildMapper(refEq(spec)) } returns jsonMapper
        every { spiedConverter.buildQueryFactory(refEq(spec), refEq(jsonMapper)) } returns queryFactory
        every {
            spiedConverter.buildQueryClient(refEq(spec), refEq(jsonMapper))
        } returns queryClient
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        @Suppress("UNCHECKED_CAST")
        spiedConverter.convert<Int, Map<String, Any?>>(
            creationContext as StepCreationContext<ElasticsearchMultiGetStepSpecificationImpl<*>>)

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(ElasticsearchDocumentsQueryStep::class).all {
                prop("id").isEqualTo("my-step")
                prop("eventsLogger").isEqualTo(eventsLogger)
                prop("meterRegistry").isNull()
                prop("retryPolicy").isEqualTo(retryPolicy)
                prop("restClientBuilder").isEqualTo(clientBuilder)
                prop("queryParamsFactory").isEqualTo(paramsFactory)
                prop("queryFactory").isSameAs(queryFactory)
            }
        }
    }

    @Test
    fun `should add meterRegistry`() = runBlockingTest {
        // given
        val spec = ElasticsearchMultiGetStepSpecificationImpl<Int>().also {
            it.name = "my-step"
            it.retry(retryPolicy)
            it.client(clientBuilder)
            it.mapper(mapperConfigurer)
            it.query(multiGetQueryBuilder)
            it.queryParameters(paramsFactory)
            it.monitoringConfig = StepMonitoringConfiguration(meters = true)
        }

        val spiedConverter = spyk(converter)
        every { spiedConverter.buildMapper(refEq(spec)) } returns jsonMapper
        every { spiedConverter.buildQueryFactory(refEq(spec), refEq(jsonMapper)) } returns queryFactory
        every {
            spiedConverter.buildQueryClient(refEq(spec), refEq(jsonMapper))
        } returns queryClient
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        @Suppress("UNCHECKED_CAST")
        spiedConverter.convert<Int, Map<String, Any?>>(
            creationContext as StepCreationContext<ElasticsearchMultiGetStepSpecificationImpl<*>>)

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(ElasticsearchDocumentsQueryStep::class).all {
                prop("id").isEqualTo("my-step")
                prop("eventsLogger").isNull()
                prop("meterRegistry").isEqualTo(meterRegistry)
                prop("retryPolicy").isEqualTo(retryPolicy)
                prop("restClientBuilder").isEqualTo(clientBuilder)
                prop("queryParamsFactory").isEqualTo(paramsFactory)
                prop("queryFactory").isSameAs(queryFactory)
            }
        }
    }

}
