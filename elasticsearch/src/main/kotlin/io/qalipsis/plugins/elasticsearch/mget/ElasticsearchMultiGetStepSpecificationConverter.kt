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

package io.qalipsis.plugins.elasticsearch.mget

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
 * [StepSpecificationConverter] from [ElasticsearchMultiGetStepSpecification] to [ElasticsearchDocumentsQueryStep]
 * to use the Multi Get API.
 *
 * @author Eric Jessé
 */
@StepConverter
internal class ElasticsearchMultiGetStepSpecificationConverter(
    @Named(Executors.IO_EXECUTOR_NAME) ioCoroutineContext: CoroutineContext,
    meterRegistry: CampaignMeterRegistry,
    eventsLogger: EventsLogger
) : AbstractElasticsearchQueryStepSpecificationConverter<ElasticsearchMultiGetStepSpecificationImpl<*>>(
    ioCoroutineContext,
    meterRegistry,
    eventsLogger
) {

    override val endpoint = "_mget"

    override val metricsQualifier = "mget"

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is ElasticsearchMultiGetStepSpecificationImpl
    }

    override fun buildQueryFactory(
        spec: ElasticsearchMultiGetStepSpecificationImpl<*>,
        jsonMapper: JsonMapper
    ): suspend (ctx: StepContext<*, *>, input: Any?) -> ObjectNode {

        @Suppress("UNCHECKED_CAST")
        val queryObjectBuilder =
            spec.queryFactory as suspend MultiGetQueryBuilder.(ctx: StepContext<*, *>, input: Any?) -> Unit
        return { context, input ->
            jsonMapper.createObjectNode().also { rootNode ->
                MultiGetQueryBuilderImpl().also { builder ->
                    builder.queryObjectBuilder(context, input)
                }.toJson(rootNode)
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun buildDocumentsExtractor(
        spec: ElasticsearchMultiGetStepSpecificationImpl<*>
    ): (JsonNode) -> List<ObjectNode> {
        return if (spec.convertFullDocument) {
            { (it.get("docs") as ArrayNode?)?.toList() as List<ObjectNode>? ?: emptyList() }
        } else {
            {
                (it.get("docs") as ArrayNode?)?.toList()?.filter { it.get("found").booleanValue() } as List<ObjectNode>?
                    ?: emptyList()
            }
        }

    }
}
