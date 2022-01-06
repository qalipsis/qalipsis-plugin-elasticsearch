package io.qalipsis.plugins.elasticsearch.save

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.jackson.modules.BeanIntrospectionModule
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.elasticsearch.Document
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope

@StepConverter
class ElasticsearchSaveStepSpecificationConverter (
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineScope: CoroutineScope,
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger
) : StepSpecificationConverter<ElasticsearchSaveStepSpecificationImpl<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is ElasticsearchSaveStepSpecificationImpl<*>
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<ElasticsearchSaveStepSpecificationImpl<*>>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name

        @Suppress("UNCHECKED_CAST")
        val step = ElasticsearchSaveStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            elasticsearchSaveQueryClient = ElasticsearchSaveQueryClientImpl(
                ioCoroutineScope,
                spec.client,
                buildMapper(spec),
                eventsLogger = supplyIf(spec.monitoringConfig.events) { eventsLogger },
                meterRegistry = supplyIf(spec.monitoringConfig.meters) { meterRegistry }
            ),
            documentsFactory = spec.documentsFactory as suspend (ctx: StepContext<*, *>, input: I) -> List<Document>
        )
        creationContext.createdStep(step)
    }

    @KTestable
    private fun buildMapper(spec: ElasticsearchSaveStepSpecificationImpl<*>): JsonMapper {
        val mapper = JsonMapper()
        mapper.registerModule(BeanIntrospectionModule())
        mapper.registerModule(JavaTimeModule())
        mapper.registerModule(KotlinModule())
        mapper.registerModule(Jdk8Module())
        spec.mapper(mapper)
        return mapper
    }
}