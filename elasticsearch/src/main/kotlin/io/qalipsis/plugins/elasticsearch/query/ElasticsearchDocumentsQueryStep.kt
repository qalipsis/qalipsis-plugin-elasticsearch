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

package io.qalipsis.plugins.elasticsearch.query

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepError
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.plugins.elasticsearch.query.model.ElasticsearchDocumentsQueryMetrics
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
    id: StepName,
    retryPolicy: RetryPolicy?,
    private val restClientBuilder: () -> RestClient,
    private val queryClient: ElasticsearchDocumentsQueryClient<T>,
    private val indicesFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<String>,
    private val queryParamsFactory: suspend (ctx: StepContext<*, *>, input: I) -> Map<String, String?>,
    private val queryFactory: suspend (ctx: StepContext<*, *>, input: I) -> ObjectNode,
    private val fetchAll: Boolean,
    private val meterRegistry: CampaignMeterRegistry?,
    private val eventsLogger: EventsLogger?
) : AbstractStep<I, Pair<I, SearchResult<T>>>(id, retryPolicy) {

    private var restClient: RestClient? = null

    private val meterPrefix: String = "elasticsearch-query"
    
    private var receivedSuccessBytesCounter: Counter? = null

    private var receivedFailureBytesCounter: Counter? = null

    private var recordsCounter: Counter? = null

    private var timeToResponse: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null

    private var documentsCounter: Counter? = null

    private var elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics? = null

    override suspend fun start(context: StepStartStopContext) {
        log.debug { "Starting step $name for campaign ${context.campaignKey} of scenario ${context.scenarioName}" }
        restClient = restClientBuilder()
        queryClient.init(restClient!!)

        initMonitoringMetrics(context)

        log.debug { "Step $name for campaign ${context.campaignKey} of scenario ${context.scenarioName} is started" }
    }

    private fun initMonitoringMetrics(context: StepStartStopContext) {
        val metersTags = context.toMetersTags()
        val scenarioName = context.scenarioName
        val stepName = context.stepName
        meterRegistry?.apply {
            recordsCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-records", metersTags).report {
                display(
                    format = "attempted req %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            receivedSuccessBytesCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-success-bytes", metersTags).report {
                display(
                    format = "\u2713 %,.0f byte successes",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 1,
                    Counter::count
                )
            }
            receivedFailureBytesCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-failure-bytes", metersTags)
            timeToResponse = meterRegistry.timer(scenarioName, stepName, "${meterPrefix}-time-to-response", metersTags)
            successCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-success", metersTags).report {
                display(
                    format = "\u2713 %,.0f successes",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 0,
                    Counter::count
                )
            }
            failureCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-failure", metersTags).report {
                display(
                    format = "\u2716 %,.0f failures",
                    severity = ReportMessageSeverity.ERROR,
                    row = 0,
                    column = 1,
                    Counter::count
                )
            }
            documentsCounter = meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-documents", metersTags)
            elasticsearchDocumentsQueryMetrics = ElasticsearchDocumentsQueryMetrics(
                receivedSuccessBytesCounter!!,
                receivedFailureBytesCounter!!,
                timeToResponse!!,
                successCounter!!,
                failureCounter!!,
                documentsCounter!!,
                recordsCounter!!
            )
        }
    }

    override suspend fun stop(context: StepStartStopContext) {
        log.debug { "Stopping step $name for campaign ${context.campaignKey} of scenario ${context.scenarioName}" }
        queryClient.cancelAll()
        kotlin.runCatching {
            restClient?.close()
        }
        stopMonitoringMetrics()
        restClient = null
        log.debug { "Step $name for campaign ${context.campaignKey} of scenario ${context.scenarioName} is stopped" }
    }

    private fun stopMonitoringMetrics() {
        meterRegistry?.apply {
            receivedSuccessBytesCounter = null
            receivedFailureBytesCounter = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
            documentsCounter = null
            recordsCounter = null
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

            val result = queryClient.execute(
                restClient!!,
                indices,
                query.toString(),
                params,
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags
            )

            val finalResult = if (fetchAll && result.isSuccess) {
                val scrollDuration = params["scroll"]
                val scrollId = result.scrollId
                if (!scrollDuration.isNullOrBlank() && !scrollId.isNullOrBlank()) {
                    scroll(result.results, scrollDuration, scrollId, eventTags)
                } else if (result.searchAfterTieBreaker?.isEmpty == false) {
                    query.set<ArrayNode>("search_after", result.searchAfterTieBreaker)
                    searchAfter(result.results, indices, query, params, eventTags)
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
        scrollId: String,
        eventTags: Map<String, String>
    ): SearchResult<T> {
        val allResults = mutableListOf<ElasticsearchDocument<T>>()
        allResults.addAll(firstQueryResults)

        var expectedResults = Int.MAX_VALUE
        var queryScrollId: String? = scrollId
        var result: SearchResult<T>

        try {
            while (expectedResults > allResults.size && !queryScrollId.isNullOrBlank()) {
                result = queryClient.scroll(
                    restClient!!,
                    scrollDuration,
                    queryScrollId,
                    elasticsearchDocumentsQueryMetrics,
                    eventsLogger,
                    eventTags
                )
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
        parameters: Map<String, String?>,
        eventTags: Map<String, String>
    ): SearchResult<T> {
        val allResults = mutableListOf<ElasticsearchDocument<T>>()
        allResults.addAll(firstQueryResults)

        var expectedResults = Int.MAX_VALUE
        var result: SearchResult<T>
        var hasTieBreaker = true

        while (expectedResults > allResults.size && hasTieBreaker) {
            // Fetched the next page.
            result = queryClient.execute(
                restClient!!,
                indices,
                query.toString(),
                parameters,
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags
            )

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
