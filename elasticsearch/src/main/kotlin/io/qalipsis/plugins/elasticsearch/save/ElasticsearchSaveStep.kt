package io.qalipsis.plugins.elasticsearch.save

import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.plugins.elasticsearch.Document

/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform inserts into Elasticsearch.
 *
 * @property elasticsearchSaveQueryClient client to save documents in Elasticsearch.
 * @property documentsFactory closure to generate a list of [Document].
 *
 * @author Alex Averyanov
 */
internal class ElasticsearchSaveStep<I>(
    id: StepName,
    retryPolicy: RetryPolicy?,
    private val elasticsearchSaveQueryClient: ElasticsearchSaveQueryClient,
    private val documentsFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<Document>,
) : AbstractStep<I, ElasticsearchSaveResult<I>>(id, retryPolicy) {

    override suspend fun start(context: StepStartStopContext) {
        init()
        elasticsearchSaveQueryClient.start(context)
    }

    override suspend fun execute(context: StepContext<I, ElasticsearchSaveResult<I>>) {
        val input = context.receive()
        val documents = documentsFactory(context, input)
        val (responseBody, meters) = elasticsearchSaveQueryClient.execute(documents, context.toEventTags())

        context.send(ElasticsearchSaveResult(input, documents, responseBody, meters))
    }

    override suspend fun stop(context: StepStartStopContext) {
        elasticsearchSaveQueryClient.stop(context)
    }
}
