package io.qalipsis.plugins.elasticsearch.query

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepError
import io.qalipsis.api.context.StepId
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import org.elasticsearch.client.RestClient
import java.time.Duration

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
    private val fetchAll: Boolean,
    private val meterRegistry: MeterRegistry?,
    private val eventsLogger: EventsLogger?
) : AbstractStep<I, Pair<I, SearchResult<T>>>(id, retryPolicy) {

    private var restClient: RestClient? = null

    private val eventPrefix = "elasticsearch.query"

    private val meterPrefix: String = "elasticsearch-query"

    private var meterTags: Tags? = null

    private var receivedSuccessBytesCounter: Counter? = null

    private var receivedFailureBytesCounter: Counter? = null

    private var timeToResponse: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null

    override suspend fun start(context: StepStartStopContext) {
        log.debug { "Starting step $id for campaign ${context.campaignId} of scenario ${context.scenarioId}" }
        restClient = restClientBuilder()

        initMonitoringMetrics(context)

        log.debug { "Step $id for campaign ${context.campaignId} of scenario ${context.scenarioId} is started" }
    }

    private fun initMonitoringMetrics(context: StepStartStopContext) {
        meterTags = context.toMetersTags()

        meterRegistry?.apply {
            receivedSuccessBytesCounter = meterRegistry.counter("${meterPrefix}-success-bytes", meterTags)
            receivedFailureBytesCounter = meterRegistry.counter("${meterPrefix}-failure-bytes", meterTags)
            timeToResponse = meterRegistry.timer("${meterPrefix}-ttr", meterTags)
            successCounter = meterRegistry.counter("${meterPrefix}-success", meterTags)
            failureCounter = meterRegistry.counter("${meterPrefix}-failure", meterTags)
        }
    }

    override suspend fun stop(context: StepStartStopContext) {
        log.debug { "Stopping step $id for campaign ${context.campaignId} of scenario ${context.scenarioId}" }
        queryClient.cancelAll()
        kotlin.runCatching {
            restClient?.close()
        }
        stopMonitoringMetrics()
        restClient = null
        log.debug { "Step $id for campaign ${context.campaignId} of scenario ${context.scenarioId} is stopped" }
    }

    private fun stopMonitoringMetrics() {
        meterRegistry?.apply {
            remove(receivedSuccessBytesCounter)
            remove(receivedFailureBytesCounter)
            remove(timeToResponse)
            remove(successCounter)
            remove(failureCounter)
            receivedSuccessBytesCounter = null
            receivedFailureBytesCounter = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
        }
    }
    override suspend fun execute(context: StepContext<I, Pair<I, SearchResult<T>>>) {
        val eventTags = context.toEventTags()
        try {
            val input = context.receive()
            val indices = indicesFactory(context, input)
            val query = queryFactory(context, input)
            val params = queryParamsFactory(context, input)

            log.debug { "Performing the request on Elasticsearch - indices: ${indices}, parameters: ${params}, query: ${query.toPrettyString()}" }

            val startTime = System.nanoTime()
            val result = queryClient.execute(restClient!!, indices, query.toString(), params)
            timeToResponse?.record(Duration.ofNanos(System.nanoTime() - startTime))

            val totalBytes = result.results.map { it.value.toString().toByteArray().size }.sum()
            val totalRecords = result.results.size.toDouble()

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
                receivedSuccessBytesCounter?.increment(totalBytes.toDouble())
                successCounter?.increment(totalRecords)
                eventsLogger?.apply {
                    info("${eventPrefix}.success.bytes", totalBytes, tags = eventTags)
                    info("${eventPrefix}.success.records", totalRecords, tags = eventTags)
                }
                context.send(input to finalResult)
            } else {
                receivedFailureBytesCounter?.increment(totalBytes.toDouble())
                failureCounter?.increment(totalRecords)
                eventsLogger?.apply {
                    info("${eventPrefix}.failure.bytes", totalBytes, tags = eventTags)
                    info("${eventPrefix}.failure.records", totalRecords, tags = eventTags)
                }
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