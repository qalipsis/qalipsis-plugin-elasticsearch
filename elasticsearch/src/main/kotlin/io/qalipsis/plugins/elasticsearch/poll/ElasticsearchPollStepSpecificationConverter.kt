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
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.elasticsearch.converters.JsonObjectListBatchConverter
import io.qalipsis.plugins.elasticsearch.converters.JsonObjectListSingleConverter
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
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger
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
            mapper,
            { Channel(Channel.UNLIMITED) },
            meterRegistry.takeIf { spec.monitoringConfig.meters },
            eventsLogger.takeIf { spec.monitoringConfig.events }
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
    fun buildConverter(
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
