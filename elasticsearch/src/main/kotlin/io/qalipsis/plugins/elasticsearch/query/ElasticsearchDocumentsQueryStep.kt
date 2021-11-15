package io.qalipsis.plugins.elasticsearch.query

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepError
import io.qalipsis.api.context.StepId
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import org.elasticsearch.client.RestClient

/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform any kind of query to fetch documents from Elasticsearch.
 *
 * @property restClientBuilder supplier for the Rest client
 * @property queryClient client to use to execute the search for the current step
 * @property indicesFactory closure to generate the list of indices to use as target
 * @property queryParamsFactory closure to generate the collection of key/value pairs for the query parameters
 * @property queryFactory closure to generate the JSON string for request body
 *
 * @author Eric Jessé
 */
internal class ElasticsearchDocumentsQueryStep<I, T>(
    id: StepId,
    retryPolicy: RetryPolicy?,
    private val restClientBuilder: () -> RestClient,
    private val queryClient: ElasticsearchDocumentsQueryClient<T>,
    private val indicesFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<String>,
    private val queryParamsFactory: suspend (ctx: StepContext<*, *>, input: I) -> Map<String, String?>,
    private val queryFactory: suspend (ctx: StepContext<*, *>, input: I) -> ObjectNode,
    private val fetchAll: Boolean
) : AbstractStep<I, Pair<I, SearchResult<T>>>(id, retryPolicy) {

    private var restClient: RestClient? = null

    override suspend fun start(context: StepStartStopContext) {
        log.debug { "Starting step $id for campaign ${context.campaignId} of scenario ${context.scenarioId}" }
        restClient = restClientBuilder()
        log.debug { "Step $id for campaign ${context.campaignId} of scenario ${context.scenarioId} is started" }
    }

    override suspend fun stop(context: StepStartStopContext) {
        log.debug { "Stopping step $id for campaign ${context.campaignId} of scenario ${context.scenarioId}" }
        queryClient.cancelAll()
        kotlin.runCatching {
            restClient?.close()
        }
        restClient = null
        log.debug { "Step $id for campaign ${context.campaignId} of scenario ${context.scenarioId} is stopped" }
    }

    override suspend fun execute(context: StepContext<I, Pair<I, SearchResult<T>>>) {
        try {
            val input = context.receive()
            val indices = indicesFactory(context, input)
            val query = queryFactory(context, input)
            val params = queryParamsFactory(context, input)

            log.debug { "Performing the request on Elasticsearch - indices: ${indices}, parameters: ${params}, query: ${query.toPrettyString()}" }

            val result = queryClient.execute(restClient!!, indices, query.toString(), params)
            val finalResult = if (fetchAll && result.isSuccess) {
                val scrollDuration = params["scroll"]
                val scrollId = result.scrollId
                if (!scrollDuration.isNullOrBlank() && !scrollId.isNullOrBlank()) {
                    scroll(result.results, scrollDuration, scrollId)
                } else if (result.searchAfterTieBreaker?.isEmpty == false) {
                    query.set<ArrayNode>("search_after", result.searchAfterTieBreaker)
                    searchAfter(result.results, indices, query, params)
                } else {
                    result
                }
            } else {
                result
            }
            if (finalResult.isSuccess) {
                context.send(input to finalResult)
            } else {
                context.addError(StepError(finalResult.failure!!))
            }
        } catch (e: Exception) {
            context.addError(StepError(e))
        }
    }

    /**
     * Fetches all the document using the Scroll API.
     */
    private suspend fun scroll(
        firstQueryResults: List<ElasticsearchDocument<T>>, scrollDuration: String,
        scrollId: String
    ): SearchResult<T> {
        val allResults = mutableListOf<ElasticsearchDocument<T>>()
        allResults.addAll(firstQueryResults)

        var expectedResults = Int.MAX_VALUE
        var queryScrollId: String? = scrollId
        var result: SearchResult<T>

        try {
            while (expectedResults > allResults.size && !queryScrollId.isNullOrBlank()) {
                result = queryClient.scroll(restClient!!, scrollDuration, queryScrollId)
                if (result.isSuccess) {
                    allResults.addAll(result.results)
                    queryScrollId = result.scrollId
                    expectedResults = result.totalResults
                } else {
                    throw result.failure!!
                }
            }
        } finally {
            queryScrollId?.let { queryClient.clearScroll(restClient!!, it) }
        }

        return SearchResult(allResults.size, allResults)
    }

    /**
     * Fetches all the document using the search after API.
     */
    private suspend fun searchAfter(
        firstQueryResults: List<ElasticsearchDocument<T>>, indices: List<String>,
        query: ObjectNode,
        parameters: Map<String, String?>
    ): SearchResult<T> {
        val allResults = mutableListOf<ElasticsearchDocument<T>>()
        allResults.addAll(firstQueryResults)

        var expectedResults = Int.MAX_VALUE
        var result: SearchResult<T>
        var hasTieBreaker = true

        while (expectedResults > allResults.size && hasTieBreaker) {
            // Fetched the next page.
            result = queryClient.execute(restClient!!, indices, query.toString(), parameters)

            if (result.isSuccess) {
                allResults.addAll(result.results)
                expectedResults = result.totalResults
                hasTieBreaker = result.searchAfterTieBreaker?.isEmpty == false
                if (hasTieBreaker) {
                    query.remove("search_after")
                    query.set<ArrayNode>("search_after", result.searchAfterTieBreaker)
                }
            } else {
                throw result.failure!!
            }
        }
        return SearchResult(allResults.size, allResults)
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}
