package io.qalipsis.plugins.elasticsearch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.jackson.modules.BeanIntrospectionModule
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryClientImpl
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryStep
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchQueryMetrics
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass


/**
 * [StepSpecificationConverter] from [AbstractElasticsearchQueryStepSpecification] to [ElasticsearchDocumentsQueryStep]
 * to use the Search API.
 *
 * @author Eric Jessé
 */
internal abstract class AbstractElasticsearchQueryStepSpecificationConverter<S : AbstractElasticsearchQueryStepSpecification<*>>(
    private val ioCoroutineContext: CoroutineContext,
    private val meterRegistry: MeterRegistry,
) : StepSpecificationConverter<S> {

    protected abstract val endpoint: String

    protected abstract val metricsQualifier: String

    override suspend fun <I, O> convert(creationContext: StepCreationContext<S>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name
        val jsonMapper = buildMapper(spec)
        val searchMetrics = buildMetrics(stepId, spec.metrics)
        val queryClient = buildQueryClient(spec, searchMetrics, jsonMapper)

        @Suppress("UNCHECKED_CAST")
        val step = ElasticsearchDocumentsQueryStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            restClientBuilder = spec.client,
            queryClient = queryClient,
            indicesFactory = buildIndicesFactory(spec),
            queryParamsFactory = spec.paramsFactory as suspend (ctx: StepContext<*, *>, input: Any?) -> Map<String, String?>,
            queryFactory = buildQueryFactory(spec, jsonMapper),
            fetchAll = fetchAll(spec)
        )
        creationContext.createdStep(step)
    }

    abstract fun buildQueryFactory(
        spec: S,
        jsonMapper: JsonMapper
    ): suspend (ctx: StepContext<*, *>, input: Any?) -> ObjectNode

    open fun buildIndicesFactory(spec: S): suspend (ctx: StepContext<*, *>, input: Any?) -> List<String> =
        { _, _ -> emptyList() }

    abstract fun buildDocumentsExtractor(spec: S): (JsonNode) -> List<ObjectNode>

    open fun fetchAll(spec: S) = false

    internal fun buildQueryClient(
        spec: S, elasticsearchQueryMetrics: ElasticsearchQueryMetrics,
        jsonMapper: JsonMapper
    ): ElasticsearchDocumentsQueryClientImpl<Any?> {

        return ElasticsearchDocumentsQueryClientImpl(
            ioCoroutineContext = ioCoroutineContext,
            endpoint = endpoint,
            queryMetrics = elasticsearchQueryMetrics,
            jsonMapper = jsonMapper,
            documentsExtractor = buildDocumentsExtractor(spec),
            converter = buildConverter(spec.targetClass, spec.convertFullDocument, jsonMapper)
        )
    }

    internal fun buildMetrics(
        stepId: StepName?,
        metricsConfiguration: ElasticsearchSearchMetricsConfiguration
    ): ElasticsearchQueryMetrics {
        val receivedSuccessBytesCounter = if (metricsConfiguration.receivedSuccessBytesCount) {
            meterRegistry.counter("elasticsearch-$metricsQualifier-success-bytes", "step", stepId)
        } else {
            null
        }
        val receivedFailureBytesCounter = if (metricsConfiguration.receivedFailureBytesCount) {
            meterRegistry.counter("elasticsearch-$metricsQualifier-failure-bytes", "step", stepId)
        } else {
            null
        }
        val documentsCounter = if (metricsConfiguration.receivedDocumentsCount) {
            meterRegistry.counter("elasticsearch-$metricsQualifier-documents", "step", stepId)
        } else {
            null
        }
        val timeToResponse = if (metricsConfiguration.timeToResponse) {
            meterRegistry.timer("elasticsearch-$metricsQualifier-response-time", "step", stepId)
        } else {
            null
        }
        val successCounter = if (metricsConfiguration.successCount) {
            meterRegistry.counter("elasticsearch-$metricsQualifier-success", "step", stepId)
        } else {
            null
        }
        val failureCounter = if (metricsConfiguration.failureCount) {
            meterRegistry.counter("elasticsearch-$metricsQualifier-failure", "step", stepId)
        } else {
            null
        }

        return ElasticsearchQueryMetrics(
            receivedSuccessBytesCounter = receivedSuccessBytesCounter,
            receivedFailureBytesCounter = receivedFailureBytesCounter,
            documentsCounter = documentsCounter,
            timeToResponse = timeToResponse,
            successCounter = successCounter,
            failureCounter = failureCounter
        )
    }

    internal fun buildMapper(spec: S): JsonMapper {
        val mapper = JsonMapper()
        mapper.registerModule(BeanIntrospectionModule())
        mapper.registerModule(JavaTimeModule())
        mapper.registerModule(KotlinModule())
        mapper.registerModule(Jdk8Module())
        spec.mapper(mapper)
        return mapper
    }

    internal fun buildConverter(
        targetClass: KClass<*>, convertFullDocument: Boolean,
        mapper: JsonMapper
    ): (ObjectNode) -> Any? {
        return if (convertFullDocument) {
            { mapper.treeToValue(it, targetClass.java) }
        } else {
            { mapper.treeToValue(it.get("_source"), targetClass.java) }
        }
    }
}
