package io.qalipsis.plugins.elasticsearch.search

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
import io.mockk.coEvery
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
import kotlinx.coroutines.runBlocking
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass

@WithMockk
internal class ElasticsearchSearchStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<ElasticsearchSearchStepSpecificationConverter>() {

    @RelaxedMockK
    private lateinit var clientBuilder: () -> RestClient

    @RelaxedMockK
    private lateinit var mapperConfigurer: (JsonMapper) -> Unit

    private val indicesFactory: (suspend (ctx: StepContext<*, *>, input: Int) -> List<String>) = relaxedMockk()

    private val queryFactory: (suspend (ctx: StepContext<*, *>, input: Any?) -> ObjectNode) = relaxedMockk()

    private val searchQueryFactory: (suspend (ctx: StepContext<*, *>, input: Int) -> String) = relaxedMockk()

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
        assertTrue(converter.support(relaxedMockk<ElasticsearchSearchStepSpecificationImpl<*>>()))
    }

    @Test
    fun `should convert with name and retry policy`() {
        // given
        val spec = ElasticsearchSearchStepSpecificationImpl<Int>().also {
            it.name = "my-step"
            it.retry(retryPolicy)
            it.client(clientBuilder)
            it.mapper(mapperConfigurer)
            it.index(indicesFactory)
            it.query(searchQueryFactory)
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
        runBlocking {
            @Suppress("UNCHECKED_CAST")
            spiedConverter.convert<Int, Map<String, Any?>>(
                creationContext as StepCreationContext<ElasticsearchSearchStepSpecificationImpl<*>>)
        }

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(ElasticsearchDocumentsQueryStep::class).all {
                prop("name").isEqualTo("my-step")
                prop("retryPolicy").isEqualTo(retryPolicy)
                prop("restClientBuilder").isEqualTo(clientBuilder)
                prop("indicesFactory").isEqualTo(indicesFactory)
                prop("queryParamsFactory").isEqualTo(paramsFactory)
                prop("queryFactory").isSameAs(queryFactory)
            }
        }
    }

    @Test
    fun `should convert without name nor retry policy`() {
        // given
        val spec = ElasticsearchSearchStepSpecificationImpl<Int>().also {
            it.client(clientBuilder)
            it.mapper(mapperConfigurer)
            it.index(indicesFactory)
            it.query(searchQueryFactory)
            it.queryParameters(paramsFactory)
        }

        val spiedConverter = spyk(converter)
        every { spiedConverter.buildMapper(refEq(spec)) } returns jsonMapper
        every { spiedConverter.buildQueryFactory(refEq(spec), refEq(jsonMapper)) } returns queryFactory
        val stepIdSlot = slot<String>()
        every {
            spiedConverter.buildQueryClient(refEq(spec), refEq(jsonMapper))
        } returns queryClient
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        runBlocking {
            @Suppress("UNCHECKED_CAST")
            spiedConverter.convert<Int, Map<String, Any?>>(
                creationContext as StepCreationContext<ElasticsearchSearchStepSpecificationImpl<*>>)
        }

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(ElasticsearchDocumentsQueryStep::class).all {
                prop("name").isNotNull()
                prop("retryPolicy").isNull()
                prop("restClientBuilder").isEqualTo(clientBuilder)
                prop("indicesFactory").isEqualTo(indicesFactory)
                prop("queryParamsFactory").isEqualTo(paramsFactory)
                prop("queryFactory").isSameAs(queryFactory)
            }
        }
    }

    @Test
    internal fun `should build the mapper`() {
        // given
        val spec = ElasticsearchSearchStepSpecificationImpl<Int>().also {
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
        val spec = ElasticsearchSearchStepSpecificationImpl<Int>().also {
            it.targetClass = targetClass
            it.convertFullDocument = true
        }
        val spiedConverter = spyk(converter)
        every { spiedConverter.buildDocumentsExtractor(any()) } returns documentExtractor
        every {
            spiedConverter.buildConverter(refEq(targetClass), eq(true), refEq(jsonMapper))
        } returns documentConverter

        // when
        val queryClient = spiedConverter.buildQueryClient(spec, jsonMapper)

        // then
        assertThat(queryClient).all {
            prop("endpoint").isEqualTo("_search")
            prop("jsonMapper").isSameAs(jsonMapper)
            prop("documentsExtractor").isSameAs(documentExtractor)
            prop("converter").isSameAs(documentConverter)
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the query client with conversion of document source only`() {
        // given
        val spec = ElasticsearchSearchStepSpecificationImpl<Int>().also {
            it.targetClass = targetClass
            it.convertFullDocument = false
        }
        val spiedConverter = spyk(converter)
        every { spiedConverter.buildDocumentsExtractor(any()) } returns documentExtractor
        every {
            spiedConverter.buildConverter(refEq(targetClass), eq(false), refEq(jsonMapper))
        } returns documentConverter

        // when
        val queryClient = spiedConverter.buildQueryClient(spec, jsonMapper)

        // then
        assertThat(queryClient).all {
            prop("endpoint").isEqualTo("_search")
            prop("jsonMapper").isSameAs(jsonMapper)
            prop("documentsExtractor").isSameAs(documentExtractor)
            prop("converter").isSameAs(documentConverter)
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the documents extractor`() {
        // given
        val jsonMapper = JsonMapper()

        // when
        val documentsExtractor = converter.buildDocumentsExtractor(relaxedMockk())

        // then it should extract results when there are.
        val nodeWithResults = jsonMapper.readTree("""{"hits":{"hits":[{"value":1},{"value":2},{"value":3}]}}""")
        assertThat(documentsExtractor(nodeWithResults)).all {
            hasSize(3)
            index(0).transform { it.get("value").intValue() }.isEqualTo(1)
            index(1).transform { it.get("value").intValue() }.isEqualTo(2)
            index(2).transform { it.get("value").intValue() }.isEqualTo(3)
        }

        // then it should return an empty collection when the first hits node is missing.
        val emptyNode = jsonMapper.readTree("""{}""")
        assertThat(documentsExtractor(emptyNode)).isEmpty()

        // then it should return an empty collection when the second hits node is missing.
        val nodeWithOnlyRootHits = jsonMapper.readTree("""{"hits":{}}""")
        assertThat(documentsExtractor(nodeWithOnlyRootHits)).isEmpty()
    }

    @Test
    internal fun `should build the query builder`() {
        // given
        val spec = ElasticsearchSearchStepSpecificationImpl<Int>().also {
            it.query(searchQueryFactory)
        }

        // when
        val queryNodeBuilder = converter.buildQueryFactory(spec, jsonMapper)

        // then
        // Executes the query builder to verify the behavior.
        val stepContext: StepContext<Int, Map<String, Any?>> = relaxedMockk()
        coEvery { searchQueryFactory(refEq(stepContext), eq(789)) } returns "this is the query"
        val objectNode: ObjectNode = relaxedMockk()
        every { jsonMapper.readTree("this is the query") } returns objectNode

        // Executes the query builder to verify it builds the JSON request as expected.
        assertThat(runBlocking { queryNodeBuilder(stepContext, 789) }).isSameAs(objectNode)
    }

    @Test
    fun `should add eventsLogger`() {
        // given
        val spec = ElasticsearchSearchStepSpecificationImpl<Int>().also {
            it.name = "my-step"
            it.retry(retryPolicy)
            it.client(clientBuilder)
            it.mapper(mapperConfigurer)
            it.index(indicesFactory)
            it.query(searchQueryFactory)
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
        runBlocking {
            @Suppress("UNCHECKED_CAST")
            spiedConverter.convert<Int, Map<String, Any?>>(
                creationContext as StepCreationContext<ElasticsearchSearchStepSpecificationImpl<*>>)
        }

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(ElasticsearchDocumentsQueryStep::class).all {
                prop("name").isEqualTo("my-step")
                prop("eventsLogger").isEqualTo(eventsLogger)
                prop("meterRegistry").isNull()
                prop("retryPolicy").isEqualTo(retryPolicy)
                prop("restClientBuilder").isEqualTo(clientBuilder)
                prop("indicesFactory").isEqualTo(indicesFactory)
                prop("queryParamsFactory").isEqualTo(paramsFactory)
                prop("queryFactory").isSameAs(queryFactory)
            }
        }
    }

    @Test
    fun `should add meterRegistry`() {
        // given
        val spec = ElasticsearchSearchStepSpecificationImpl<Int>().also {
            it.name = "my-step"
            it.retry(retryPolicy)
            it.client(clientBuilder)
            it.mapper(mapperConfigurer)
            it.index(indicesFactory)
            it.query(searchQueryFactory)
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
        runBlocking {
            @Suppress("UNCHECKED_CAST")
            spiedConverter.convert<Int, Map<String, Any?>>(
                creationContext as StepCreationContext<ElasticsearchSearchStepSpecificationImpl<*>>)
        }

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(ElasticsearchDocumentsQueryStep::class).all {
                prop("name").isEqualTo("my-step")
                prop("eventsLogger").isNull()
                prop("meterRegistry").isEqualTo(meterRegistry)
                prop("retryPolicy").isEqualTo(retryPolicy)
                prop("restClientBuilder").isEqualTo(clientBuilder)
                prop("indicesFactory").isEqualTo(indicesFactory)
                prop("queryParamsFactory").isEqualTo(paramsFactory)
                prop("queryFactory").isSameAs(queryFactory)
            }
        }
    }
}
