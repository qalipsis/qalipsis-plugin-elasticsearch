package io.qalipsis.plugins.elasticsearch.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.getProperty
import io.aerisconsulting.catadioptre.invokeInvisible
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.context.StepName
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.elasticsearch.ElasticsearchSearchMetricsConfiguration
import io.qalipsis.plugins.elasticsearch.converters.JsonObjectListBatchConverter
import io.qalipsis.plugins.elasticsearch.converters.JsonObjectListSingleConverter
import io.qalipsis.plugins.elasticsearch.poll.catadioptre.buildMetrics
import io.qalipsis.plugins.elasticsearch.poll.catadioptre.buildStatement
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchQueryMetrics
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
import org.junit.jupiter.api.Assertions.assertEquals
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
    private lateinit var mockedQueryMetrics: ElasticsearchQueryMetrics

    @RelaxedMockK
    private lateinit var counter: Counter

    @RelaxedMockK
    private lateinit var timer: Timer

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
        val spiedConverter = spyk(converter, recordPrivateCalls = true)
        every { spiedConverter["buildMapper"](refEq(spec)) } returns mockedJsonMapper
        every { spiedConverter["buildMetrics"](any<StepName>(), refEq(spec.metrics)) } returns mockedQueryMetrics
        every {
            spiedConverter["buildStatement"](refEq(mockedQueryFactory), refEq(mockedJsonMapper))
        } returns mockedElasticsearchPollStatement
        every { spiedConverter["buildConverter"](refEq(spec), any<JsonMapper>()) } returns mockedDocumentsConverter

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
                    prop("queryMetrics").isSameAs(mockedQueryMetrics)
                    prop("jsonMapper").isSameAs(mockedJsonMapper)
                    prop("resultsChannelFactory").isNotNull()
                }
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isSameAs(mockedDocumentsConverter)
            }
        }

        verifyOnce {
            spiedConverter["buildMetrics"](eq(creationContext.createdStep!!.id), refEq(spec.metrics))
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
    internal fun `should build the poll statement`() {
        // given
        val query = """{"size":0,"query":{"bool":{"must":[{"match_all":{}}]}},"sort":"timestamp"}"""
        val queryFactory: () -> String = { query }
        val jsonMapper = spyk(JsonMapper())

        // when
        val statement = converter.buildStatement(queryFactory, jsonMapper)

        // then
        assertThat(statement).isInstanceOf(ElasticsearchPollStatementImpl::class).all {
            prop("jsonBuilder").isNotNull()
            prop(ElasticsearchPollStatementImpl::query).isEqualTo(query)
            prop(ElasticsearchPollStatementImpl::tieBreaker).isNull()
        }
        verifyOnce { jsonMapper.readTree(refEq(query)) }
        val jsonBuilder: () -> ObjectNode = statement.getProperty("jsonBuilder")
        assertEquals(jsonMapper.readTree(query), jsonBuilder())
    }

    @Test
    internal fun `should build batch converter to deserialize to a map`() {
        // given
        val spec = ElasticsearchPollStepSpecificationImpl()

        // when
        val converter = converter.invokeInvisible<DatasourceObjectConverter<List<ObjectNode>, out Any>>("buildConverter", spec, mockedJsonMapper)

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
        val converter = converter.invokeInvisible<DatasourceObjectConverter<List<ObjectNode>, out Any>>("buildConverter", spec, mockedJsonMapper)

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
        val converter = converter.invokeInvisible<DatasourceObjectConverter<List<ObjectNode>, out Any>>("buildConverter", spec, mockedJsonMapper)

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
        val converter = converter.invokeInvisible<DatasourceObjectConverter<List<ObjectNode>, out Any>>("buildConverter", spec, mockedJsonMapper)

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
        val converter = converter.invokeInvisible<DatasourceObjectConverter<List<ObjectNode>, out Any>>("buildConverter", spec, mockedJsonMapper)

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
        val converter = converter.invokeInvisible<DatasourceObjectConverter<List<ObjectNode>, out Any>>("buildConverter", spec, mockedJsonMapper)

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
    internal fun `should build the query metrics to record the bytes when success only`() {
        // given
        every { meterRegistry.counter("elasticsearch-poll-success-bytes", "step", "my-step") } returns counter

        // when
        val searchMetrics = converter.buildMetrics("my-step", ElasticsearchSearchMetricsConfiguration(
            receivedSuccessBytesCount = true
        ))

        // then
        verifyOnce { meterRegistry.counter("elasticsearch-poll-success-bytes", "step", "my-step") }
        assertThat(searchMetrics).all {
            prop("receivedSuccessBytesCounter").isSameAs(counter)
            prop("receivedFailureBytesCounter").isNull()
            prop("documentsCounter").isNull()
            prop("timeToResponse").isNull()
            prop("successCounter").isNull()
            prop("failureCounter").isNull()
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the query metrics to record the records when failure only`() {
        // given
        every { meterRegistry.counter("elasticsearch-poll-failure-bytes", "step", "my-step") } returns counter

        // when
        val searchMetrics = converter.buildMetrics("my-step", ElasticsearchSearchMetricsConfiguration(
            receivedFailureBytesCount = true
        ))

        // then
        verifyOnce { meterRegistry.counter("elasticsearch-poll-failure-bytes", "step", "my-step") }
        assertThat(searchMetrics).all {
            prop("receivedSuccessBytesCounter").isNull()
            prop("receivedFailureBytesCounter").isSameAs(counter)
            prop("documentsCounter").isNull()
            prop("timeToResponse").isNull()
            prop("successCounter").isNull()
            prop("failureCounter").isNull()
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the query metrics to record the received documents`() {
        // given
        every { meterRegistry.counter("elasticsearch-poll-documents", "step", "my-step") } returns counter

        // when
        val searchMetrics = converter.buildMetrics("my-step", ElasticsearchSearchMetricsConfiguration(
            receivedDocumentsCount = true
        ))

        // then
        verifyOnce { meterRegistry.counter("elasticsearch-poll-documents", "step", "my-step") }
        assertThat(searchMetrics).all {
            prop("receivedSuccessBytesCounter").isNull()
            prop("receivedFailureBytesCounter").isNull()
            prop("documentsCounter").isSameAs(counter)
            prop("timeToResponse").isNull()
            prop("successCounter").isNull()
            prop("failureCounter").isNull()
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the query metrics to record the time to response only`() {
        // given
        every { meterRegistry.timer("elasticsearch-poll-response-time", "step", "my-step") } returns timer

        // when
        val searchMetrics = converter.buildMetrics("my-step", ElasticsearchSearchMetricsConfiguration(
            timeToResponse = true
        ))

        // then
        verifyOnce { meterRegistry.timer("elasticsearch-poll-response-time", "step", "my-step") }
        assertThat(searchMetrics).all {
            prop("receivedSuccessBytesCounter").isNull()
            prop("receivedFailureBytesCounter").isNull()
            prop("documentsCounter").isNull()
            prop("timeToResponse").isSameAs(timer)
            prop("successCounter").isNull()
            prop("failureCounter").isNull()
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the query metrics to record the successes only`() {
        // given
        every { meterRegistry.counter("elasticsearch-poll-success", "step", "my-step") } returns counter

        // when
        val searchMetrics = converter.buildMetrics("my-step", ElasticsearchSearchMetricsConfiguration(
            successCount = true
        ))

        // then
        verifyOnce { meterRegistry.counter("elasticsearch-poll-success", "step", "my-step") }
        assertThat(searchMetrics).all {
            prop("receivedSuccessBytesCounter").isNull()
            prop("receivedFailureBytesCounter").isNull()
            prop("documentsCounter").isNull()
            prop("timeToResponse").isNull()
            prop("successCounter").isSameAs(counter)
            prop("failureCounter").isNull()
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the query metrics to record the failures only`() {
        // given
        every { meterRegistry.counter("elasticsearch-poll-failure", "step", "my-step") } returns counter

        // when
        val searchMetrics = converter.buildMetrics("my-step", ElasticsearchSearchMetricsConfiguration(
            failureCount = true
        ))

        // then
        verifyOnce { meterRegistry.counter("elasticsearch-poll-failure", "step", "my-step") }
        assertThat(searchMetrics).all {
            prop("receivedSuccessBytesCounter").isNull()
            prop("receivedFailureBytesCounter").isNull()
            prop("documentsCounter").isNull()
            prop("timeToResponse").isNull()
            prop("successCounter").isNull()
            prop("failureCounter").isSameAs(counter)
        }
        confirmVerified(meterRegistry)
    }

    @Test
    internal fun `should build the query metrics with all the metrics`() {
        // when
        val searchMetrics = converter.buildMetrics("my-step", ElasticsearchSearchMetricsConfiguration(
        ).also { it.all() })

        // then
        verifyOnce { meterRegistry.counter("elasticsearch-poll-success-bytes", "step", "my-step") }
        verifyOnce { meterRegistry.counter("elasticsearch-poll-failure-bytes", "step", "my-step") }
        verifyOnce { meterRegistry.counter("elasticsearch-poll-documents", "step", "my-step") }
        verifyOnce { meterRegistry.timer("elasticsearch-poll-response-time", "step", "my-step") }
        verifyOnce { meterRegistry.counter("elasticsearch-poll-success", "step", "my-step") }
        verifyOnce { meterRegistry.counter("elasticsearch-poll-failure", "step", "my-step") }

        assertThat(searchMetrics).all {
            prop("receivedSuccessBytesCounter").isNotNull()
            prop("receivedFailureBytesCounter").isNotNull()
            prop("documentsCounter").isNotNull()
            prop("timeToResponse").isNotNull()
            prop("successCounter").isNotNull()
            prop("failureCounter").isNotNull()
        }
        confirmVerified(meterRegistry)
    }
}
