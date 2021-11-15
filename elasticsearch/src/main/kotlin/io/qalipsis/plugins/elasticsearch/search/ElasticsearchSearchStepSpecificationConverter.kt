package io.qalipsis.plugins.elasticsearch.search

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
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
    meterRegistry: MeterRegistry
) : AbstractElasticsearchQueryStepSpecificationConverter<ElasticsearchSearchStepSpecificationImpl<*>>(
    ioCoroutineContext,
    meterRegistry
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
