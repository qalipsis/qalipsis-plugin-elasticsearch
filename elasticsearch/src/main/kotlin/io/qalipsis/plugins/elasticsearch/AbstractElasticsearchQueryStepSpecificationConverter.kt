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

package io.qalipsis.plugins.elasticsearch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.micronaut.jackson.modules.BeanIntrospectionModule
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryClientImpl
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryStep
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
    private val meterRegistry: CampaignMeterRegistry,
    private val eventsLogger: EventsLogger
) : StepSpecificationConverter<S> {

    protected abstract val endpoint: String

    protected abstract val metricsQualifier: String

    override suspend fun <I, O> convert(creationContext: StepCreationContext<S>) {
        val spec = creationContext.stepSpecification

        val stepId = spec.name
        val jsonMapper = buildMapper(spec)
        val queryClient = buildQueryClient(spec, jsonMapper)

        @Suppress("UNCHECKED_CAST")
        val step = ElasticsearchDocumentsQueryStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            restClientBuilder = spec.client,
            queryClient = queryClient,
            indicesFactory = buildIndicesFactory(spec),
            queryParamsFactory = spec.paramsFactory as suspend (ctx: StepContext<*, *>, input: Any?) -> Map<String, String?>,
            queryFactory = buildQueryFactory(spec, jsonMapper),
            fetchAll = fetchAll(spec),
            meterRegistry = meterRegistry.takeIf { spec.monitoringConfig.meters },
            eventsLogger = eventsLogger.takeIf { spec.monitoringConfig.events }
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
        spec: S,
        jsonMapper: JsonMapper
    ): ElasticsearchDocumentsQueryClientImpl<Any?> {

        return ElasticsearchDocumentsQueryClientImpl(
            ioCoroutineContext = ioCoroutineContext,
            endpoint = endpoint,
            jsonMapper = jsonMapper,
            documentsExtractor = buildDocumentsExtractor(spec),
            converter = buildConverter(spec.targetClass, spec.convertFullDocument, jsonMapper)
        )
    }

    internal fun buildMapper(spec: S): JsonMapper {
        val mapper = JsonMapper()
        mapper.registerModule(BeanIntrospectionModule())
        mapper.registerModule(JavaTimeModule())
        mapper.registerModule(KotlinModule.Builder().build())
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
