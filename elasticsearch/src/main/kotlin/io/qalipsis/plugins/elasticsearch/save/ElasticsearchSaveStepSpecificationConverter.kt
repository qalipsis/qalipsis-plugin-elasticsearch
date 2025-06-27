/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.elasticsearch.save

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.aerisconsulting.catadioptre.KTestable
import io.micronaut.jackson.modules.BeanIntrospectionModule
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.elasticsearch.Document
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope

@StepConverter
internal class ElasticsearchSaveStepSpecificationConverter(
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineScope: CoroutineScope,
    private val meterRegistry: CampaignMeterRegistry,
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
                buildMapper(),
                spec.keepResponse,
                eventsLogger = supplyIf(spec.monitoringConfig.events) { eventsLogger },
                meterRegistry = supplyIf(spec.monitoringConfig.meters) { meterRegistry }
            ),
            documentsFactory = spec.documentsFactory as suspend (ctx: StepContext<*, *>, input: I) -> List<Document>
        )
        creationContext.createdStep(step)
    }

    @KTestable
    private fun buildMapper(): JsonMapper {
        val mapper = JsonMapper()
        mapper.registerModule(BeanIntrospectionModule())
        mapper.registerModule(JavaTimeModule())
        mapper.registerModule(KotlinModule.Builder().build())
        mapper.registerModule(Jdk8Module())
        return mapper
    }
}