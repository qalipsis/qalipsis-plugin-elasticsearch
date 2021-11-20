package io.qalipsis.plugins.elasticsearch.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import assertk.assertions.key
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.getProperty
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.elasticsearch.converters.JsonObjectListBatchConverter
import io.qalipsis.plugins.elasticsearch.converters.JsonObjectListSingleConverter
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.assertk.typedProp
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyOnce
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runBlockingTest
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Random
import kotlin.coroutines.CoroutineContext

/**
 *
 * @author Eric Jessé
 */
@WithMockk
internal class ElasticsearchPollStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<ElasticsearchPollStepSpecificationConverter>() {

    @RelaxedMockK
    private lateinit var mockedJsonMapper: JsonMapper

    @RelaxedMockK
    private lateinit var restClientBuilder: () -> RestClient

    @RelaxedMockK
    private lateinit var mockedElasticsearchPollStatement: ElasticsearchPollStatement

    @RelaxedMockK
    private lateinit var mockedMapperConfigurer: (JsonMapper) -> Unit

    @RelaxedMockK
    private lateinit var mockedQueryFactory: () -> String

    @RelaxedMockK
    private lateinit var mockedDocumentsConverter: DatasourceObjectConverter<List<ObjectNode>, out Any>

    // Class<*> cannot be mocked, so a "random" class is used.
    private val targetJavaClass: Class<*> = Random::class.java

    // Class<*> cannot be mocked, so a "random" class is used.
    private val targetClass = Random::class

    @RelaxedMockK
    private lateinit var ioCoroutineScope: CoroutineScope

    @RelaxedMockK
    private lateinit var ioCoroutineContext: CoroutineContext

    @Test
    override fun `should not support unexpected spec`() {
        assertFalse(converter.support(relaxedMockk()))
    }

    @Test
    override fun `should support expected spec`() {
        assertTrue(converter.support(relaxedMockk<ElasticsearchPollStepSpecificationImpl>()))
    }

    @Test
    @ExperimentalCoroutinesApi
    fun `should convert with name`() = runBlockingTest {
        // given
        val spec = ElasticsearchPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            this.client = restClientBuilder
            this.mapper = mockedMapperConfigurer
            this.indices.clear()
            this.indices.add("index-1")
            this.indices.add("ind*2")
            this.queryParameters.putAll(arrayOf("param-1" to "val-1", "param-2" to "val-2"))
            this.queryFactory = mockedQueryFactory
            this.pollDelay = Duration.ofSeconds(23)
            broadcast(123, Duration.ofSeconds(20))
        }
        val spiedConverter = spyk(converter)
        every { spiedConverter["buildMapper"](refEq(spec)) } returns mockedJsonMapper
        every {
            spiedConverter["buildStatement"](refEq(mockedQueryFactory), refEq(mockedJsonMapper))
        } returns mockedElasticsearchPollStatement
        every { spiedConverter.buildConverter(refEq(spec), any()) } returns mockedDocumentsConverter

        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(creationContext)

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("id").isEqualTo("my-step")
                prop("reader").isNotNull().isInstanceOf(ElasticsearchIterativeReader::class).all {
                    prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                    prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                    prop("restClientBuilder").isSameAs(restClientBuilder)
                    prop("elasticsearchPollStatement").isSameAs(mockedElasticsearchPollStatement)
                    typedProp<Map<String, String>>("queryParams").all {
                        hasSize(2)
                        key("param-1").isEqualTo("val-1")
                        key("param-2").isEqualTo("val-2")
                    }
                    prop("index").isEqualTo("index-1,ind*2")
                    prop("pollDelay").isEqualTo(Duration.ofSeconds(23))
                    prop("jsonMapper").isSameAs(mockedJsonMapper)
                    prop("resultsChannelFactory").isNotNull()
                }
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isSameAs(mockedDocumentsConverter)
            }
        }
        val channelFactory = creationContext.createdStep!!
            .getProperty<ElasticsearchIterativeReader>("reader")
            .getProperty<() -> Channel<List<ObjectNode>>>("resultsChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

    @Test
    internal fun `should build batch converter to deserialize to a map`() {
        // given
        val spec = ElasticsearchPollStepSpecificationImpl()

        // when
        val converter = converter.buildConverter(spec, mockedJsonMapper)

        // then
        assertThat(converter).isInstanceOf(JsonObjectListBatchConverter::class).all {
            prop("converter").isNotNull()
        }
        val jsonConverter = converter.getProperty<(ObjectNode) -> Map<String, Any?>>("converter")
        val sourceNode: JsonNode = relaxedMockk()
        val objectNode: ObjectNode = relaxedMockk()
        every { objectNode.get("_source") } returns sourceNode
        jsonConverter(objectNode)
        verifyOnce { mockedJsonMapper.treeToValue(refEq(sourceNode), Map::class.java) }
    }

    @Test
    internal fun `should build batch converter to deserialize to the expected class`() {
        // given
        val spec = ElasticsearchPollStepSpecificationImpl().also {
            it.deserialize(targetClass)
        }

        // when
        val converter = converter.buildConverter(spec, mockedJsonMapper)

        // then
        assertThat(converter).isInstanceOf(JsonObjectListBatchConverter::class).all {
            prop("converter").isNotNull()
        }
        val jsonConverter = converter.getProperty<(ObjectNode) -> Map<String, Any?>>("converter")
        val sourceNode: JsonNode = relaxedMockk()
        val objectNode: ObjectNode = relaxedMockk()
        every { objectNode.get("_source") } returns sourceNode
        jsonConverter(objectNode)
        verifyOnce { mockedJsonMapper.treeToValue(refEq(sourceNode), refEq(targetJavaClass)) }
    }

    @Test
    internal fun `should build batch converter to deserialize to the expected class for the full document`() {
        // given
        val spec = ElasticsearchPollStepSpecificationImpl().also {
            it.deserialize(targetClass, true)
        }

        // when
        val converter = converter.buildConverter(spec, mockedJsonMapper)

        // then
        assertThat(converter).isInstanceOf(JsonObjectListBatchConverter::class).all {
            prop("converter").isNotNull()
        }
        val jsonConverter = converter.getProperty<(ObjectNode) -> Map<String, Any?>>("converter")
        val objectNode: ObjectNode = relaxedMockk()
        jsonConverter(objectNode)
        verifyOnce { mockedJsonMapper.treeToValue(refEq(objectNode), refEq(targetJavaClass)) }
    }

    @Test
    internal fun `should build single converter to deserialize to a map`() {
        // given
        val spec = ElasticsearchPollStepSpecificationImpl().also {
            it.flatten()
        }

        // when
        val converter = converter.buildConverter(spec, mockedJsonMapper)

        // then
        assertThat(converter).isInstanceOf(JsonObjectListSingleConverter::class).all {
            prop("converter").isNotNull()
        }
        val jsonConverter = converter.getProperty<(ObjectNode) -> Map<String, Any?>>("converter")
        val sourceNode: JsonNode = relaxedMockk()
        val objectNode: ObjectNode = relaxedMockk()
        every { objectNode.get("_source") } returns sourceNode
        jsonConverter(objectNode)
        verifyOnce { mockedJsonMapper.treeToValue(refEq(sourceNode), Map::class.java) }
    }

    @Test
    internal fun `should build single converter to deserialize to the expected class`() {
        // given
        val spec = ElasticsearchPollStepSpecificationImpl().also {
            it.flatten(targetClass)
        }

        // when
        val converter = converter.buildConverter(spec, mockedJsonMapper)

        // then
        assertThat(converter).isInstanceOf(JsonObjectListSingleConverter::class).all {
            prop("converter").isNotNull()
        }
        val jsonConverter = converter.getProperty<(ObjectNode) -> Map<String, Any?>>("converter")
        val sourceNode: JsonNode = relaxedMockk()
        val objectNode: ObjectNode = relaxedMockk()
        every { objectNode.get("_source") } returns sourceNode
        jsonConverter(objectNode)
        verifyOnce { mockedJsonMapper.treeToValue(refEq(sourceNode), refEq(targetJavaClass)) }
    }

    @Test
    internal fun `should build single converter to deserialize to the expected class for the full document`() {
        // given
        val spec = ElasticsearchPollStepSpecificationImpl().also {
            it.flatten(targetClass, true)
        }

        // when
        val converter = converter.buildConverter(spec, mockedJsonMapper)

        // then
        assertThat(converter).isInstanceOf(JsonObjectListSingleConverter::class).all {
            prop("converter").isNotNull()
        }
        val jsonConverter = converter.getProperty<(ObjectNode) -> Map<String, Any?>>("converter")
        val objectNode: ObjectNode = relaxedMockk()
        jsonConverter(objectNode)
        verifyOnce { mockedJsonMapper.treeToValue(refEq(objectNode), refEq(targetJavaClass)) }
    }

    @Test
    @ExperimentalCoroutinesApi
    fun `should add eventsLogger`() = runBlockingTest {

        // given
        val spec = ElasticsearchPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            this.client = restClientBuilder
            this.mapper = mockedMapperConfigurer
            this.indices.clear()
            this.indices.add("index-1")
            this.indices.add("ind*2")
            this.queryParameters.putAll(arrayOf("param-1" to "val-1", "param-2" to "val-2"))
            this.queryFactory = mockedQueryFactory
            this.pollDelay = Duration.ofSeconds(23)
            this.monitoringConfig = StepMonitoringConfiguration(events = true)
            broadcast(123, Duration.ofSeconds(20))
        }
        val spiedConverter = spyk(converter)
        every { spiedConverter["buildMapper"](refEq(spec)) } returns mockedJsonMapper
        every {
            spiedConverter["buildStatement"](refEq(mockedQueryFactory), refEq(mockedJsonMapper))
        } returns mockedElasticsearchPollStatement
        every { spiedConverter.buildConverter(refEq(spec), any()) } returns mockedDocumentsConverter

        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(creationContext)

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("id").isEqualTo("my-step")
                prop("reader").isNotNull().isInstanceOf(ElasticsearchIterativeReader::class).all {
                    prop("eventsLogger").isSameAs(eventsLogger)
                    prop("meterRegistry").isNull()
                    prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                    prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                    prop("restClientBuilder").isSameAs(restClientBuilder)
                    prop("elasticsearchPollStatement").isSameAs(mockedElasticsearchPollStatement)
                    typedProp<Map<String, String>>("queryParams").all {
                        hasSize(2)
                        key("param-1").isEqualTo("val-1")
                        key("param-2").isEqualTo("val-2")
                    }
                    prop("index").isEqualTo("index-1,ind*2")
                    prop("pollDelay").isEqualTo(Duration.ofSeconds(23))
                    prop("jsonMapper").isSameAs(mockedJsonMapper)
                    prop("resultsChannelFactory").isNotNull()
                }
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isSameAs(mockedDocumentsConverter)
            }
        }
        val channelFactory = creationContext.createdStep!!
            .getProperty<ElasticsearchIterativeReader>("reader")
            .getProperty<() -> Channel<List<ObjectNode>>>("resultsChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

    @Test
    @ExperimentalCoroutinesApi
    fun `should add meterRegistry`() = runBlockingTest {

        // given
        val spec = ElasticsearchPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            this.client = restClientBuilder
            this.mapper = mockedMapperConfigurer
            this.indices.clear()
            this.indices.add("index-1")
            this.indices.add("ind*2")
            this.queryParameters.putAll(arrayOf("param-1" to "val-1", "param-2" to "val-2"))
            this.queryFactory = mockedQueryFactory
            this.pollDelay = Duration.ofSeconds(23)
            this.monitoringConfig = StepMonitoringConfiguration(meters = true)
            broadcast(123, Duration.ofSeconds(20))
        }
        val spiedConverter = spyk(converter)
        every { spiedConverter["buildMapper"](refEq(spec)) } returns mockedJsonMapper
        every {
            spiedConverter["buildStatement"](refEq(mockedQueryFactory), refEq(mockedJsonMapper))
        } returns mockedElasticsearchPollStatement
        every { spiedConverter.buildConverter(refEq(spec), any()) } returns mockedDocumentsConverter

        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(creationContext)

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("id").isEqualTo("my-step")
                prop("reader").isNotNull().isInstanceOf(ElasticsearchIterativeReader::class).all {
                    prop("eventsLogger").isNull()
                    prop("meterRegistry").isSameAs(meterRegistry)
                    prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                    prop("ioCoroutineContext").isSameAs(ioCoroutineContext)
                    prop("restClientBuilder").isSameAs(restClientBuilder)
                    prop("elasticsearchPollStatement").isSameAs(mockedElasticsearchPollStatement)
                    typedProp<Map<String, String>>("queryParams").all {
                        hasSize(2)
                        key("param-1").isEqualTo("val-1")
                        key("param-2").isEqualTo("val-2")
                    }
                    prop("index").isEqualTo("index-1,ind*2")
                    prop("pollDelay").isEqualTo(Duration.ofSeconds(23))
                    prop("jsonMapper").isSameAs(mockedJsonMapper)
                    prop("resultsChannelFactory").isNotNull()
                }
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isSameAs(mockedDocumentsConverter)
            }
        }
        val channelFactory = creationContext.createdStep!!
            .getProperty<ElasticsearchIterativeReader>("reader")
            .getProperty<() -> Channel<List<ObjectNode>>>("resultsChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }
}
