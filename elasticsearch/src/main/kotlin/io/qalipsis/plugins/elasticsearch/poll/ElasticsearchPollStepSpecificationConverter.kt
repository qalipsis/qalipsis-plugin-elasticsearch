/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.aerisconsulting.catadioptre.KTestable
import io.micronaut.jackson.modules.BeanIntrospectionModule
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
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
    private val meterRegistry: CampaignMeterRegistry,
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
        mapper.registerModule(KotlinModule.Builder().build())
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
