package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.jackson.modules.BeanIntrospectionModule
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepName
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.elasticsearch.ElasticsearchSearchMetricsConfiguration
import io.qalipsis.plugins.elasticsearch.converters.JsonObjectListBatchConverter
import io.qalipsis.plugins.elasticsearch.converters.JsonObjectListSingleConverter
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchQueryMetrics
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlin.coroutines.CoroutineContext


/**
 * [StepSpecificationConverter] from [ElasticsearchPollStepSpecificationImpl] to [ElasticsearchIterativeReader].
 *
 * @author Eric Jessé
 */
@StepConverter
internal class ElasticsearchPollStepSpecificationConverter(
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineScope: CoroutineScope,
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineContext: CoroutineContext,
    private val meterRegistry: MeterRegistry
) : StepSpecificationConverter<ElasticsearchPollStepSpecificationImpl> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is ElasticsearchPollStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<ElasticsearchPollStepSpecificationImpl>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name

        val mapper = buildMapper(spec)
        val sqlStatement = buildStatement(spec.queryFactory, mapper)

        val reader = ElasticsearchIterativeReader(
            ioCoroutineScope,
            ioCoroutineContext,
            spec.client,
            sqlStatement,
            spec.indices.joinToString(",") { it.trim() },
            spec.queryParameters,
            spec.pollDelay!!,
            buildMetrics(stepId, spec.metrics),
            mapper,
            { Channel(Channel.UNLIMITED) }
        )
        val step = IterativeDatasourceStep(
            stepId,
            reader,
            NoopDatasourceObjectProcessor(),
            buildConverter(spec, mapper)
        )
        creationContext.createdStep(step)
    }

    @KTestable
    private fun buildMapper(spec: ElasticsearchPollStepSpecificationImpl): JsonMapper {
        val mapper = JsonMapper()
        mapper.registerModule(BeanIntrospectionModule())
        mapper.registerModule(JavaTimeModule())
        mapper.registerModule(KotlinModule())
        mapper.registerModule(Jdk8Module())
        spec.mapper(mapper)
        return mapper
    }

    @KTestable
    private fun buildStatement(queryBuilder: () -> String, mapper: JsonMapper): ElasticsearchPollStatement {
        return ElasticsearchPollStatementImpl {
            mapper.readTree(queryBuilder()) as ObjectNode
        }
    }

    @KTestable
    private fun buildMetrics(
        stepId: StepName?,
        metricsConfiguration: ElasticsearchSearchMetricsConfiguration
    ): ElasticsearchQueryMetrics {
        val receivedSuccessBytesCounter = if (metricsConfiguration.receivedSuccessBytesCount) {
            meterRegistry.counter("elasticsearch-poll-success-bytes", "step", stepId)
        } else {
            null
        }
        val receivedFailureBytesCounter = if (metricsConfiguration.receivedFailureBytesCount) {
            meterRegistry.counter("elasticsearch-poll-failure-bytes", "step", stepId)
        } else {
            null
        }
        val documentsCounter = if (metricsConfiguration.receivedDocumentsCount) {
            meterRegistry.counter("elasticsearch-poll-documents", "step", stepId)
        } else {
            null
        }
        val timeToResponse = if (metricsConfiguration.timeToResponse) {
            meterRegistry.timer("elasticsearch-poll-response-time", "step", stepId)
        } else {
            null
        }
        val successCounter = if (metricsConfiguration.successCount) {
            meterRegistry.counter("elasticsearch-poll-success", "step", stepId)
        } else {
            null
        }
        val failureCounter = if (metricsConfiguration.failureCount) {
            meterRegistry.counter("elasticsearch-poll-failure", "step", stepId)
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

    private fun buildConverter(
        spec: ElasticsearchPollStepSpecificationImpl,
        mapper: JsonMapper
    ): DatasourceObjectConverter<List<ObjectNode>, out Any> {
        val targetClass = spec.targetClass.java
        val objectNodeConverter: (ObjectNode) -> Any = if (spec.convertFullDocument) {
            { mapper.treeToValue(it, targetClass) }
        } else {
            { mapper.treeToValue(it.get("_source"), targetClass) }
        }

        return if (spec.flattenOutput) {
            JsonObjectListSingleConverter(objectNodeConverter)
        } else {
            JsonObjectListBatchConverter(objectNodeConverter)
        }
    }
}
