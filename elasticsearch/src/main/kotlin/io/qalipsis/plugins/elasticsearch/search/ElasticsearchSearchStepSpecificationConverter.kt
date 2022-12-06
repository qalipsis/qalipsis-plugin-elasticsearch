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

package io.qalipsis.plugins.elasticsearch.search

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.elasticsearch.AbstractElasticsearchQueryStepSpecificationConverter
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryStep
import jakarta.inject.Named
import kotlin.coroutines.CoroutineContext


/**
 * [StepSpecificationConverter] from [ElasticsearchSearchStepSpecificationImpl] to [ElasticsearchDocumentsQueryStep]
 * to use the Search API.
 *
 * @author Eric Jessé
 */
@StepConverter
internal class ElasticsearchSearchStepSpecificationConverter(
    @Named(Executors.IO_EXECUTOR_NAME) ioCoroutineContext: CoroutineContext,
    meterRegistry: CampaignMeterRegistry,
    eventsLogger: EventsLogger
) : AbstractElasticsearchQueryStepSpecificationConverter<ElasticsearchSearchStepSpecificationImpl<*>>(
    ioCoroutineContext,
    meterRegistry,
    eventsLogger
) {

    override val endpoint = "_search"

    override val metricsQualifier = "search"

    override fun fetchAll(spec: ElasticsearchSearchStepSpecificationImpl<*>) = spec.fetchAll

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is ElasticsearchSearchStepSpecificationImpl
    }

    override fun buildQueryFactory(
        spec: ElasticsearchSearchStepSpecificationImpl<*>,
        jsonMapper: JsonMapper
    ): suspend (ctx: StepContext<*, *>, input: Any?) -> ObjectNode {
        @Suppress("UNCHECKED_CAST")
        val queryStringBuilder = spec.queryFactory as suspend (ctx: StepContext<*, *>, input: Any?) -> String
        return { context, input ->
            jsonMapper.readTree(queryStringBuilder(context, input)) as ObjectNode
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun buildIndicesFactory(
        spec: ElasticsearchSearchStepSpecificationImpl<*>
    ): suspend (ctx: StepContext<*, *>, input: Any?) -> List<String> {
        return spec.indicesFactory as suspend (StepContext<*, *>, Any?) -> List<String>
    }

    @Suppress("UNCHECKED_CAST")
    override fun buildDocumentsExtractor(
        spec: ElasticsearchSearchStepSpecificationImpl<*>
    ): (JsonNode) -> List<ObjectNode> {
        return {
            (it.get("hits")?.get("hits") as ArrayNode?)?.let { arrayNode ->
                arrayNode.toList() as List<ObjectNode>
            } ?: emptyList()
        }
    }
}
