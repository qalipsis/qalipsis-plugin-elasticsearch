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

package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import io.qalipsis.api.sync.ImmutableSlot
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Cancellable
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.ResponseListener
import org.elasticsearch.client.RestClient
import java.time.Duration
import kotlin.coroutines.CoroutineContext

/**
 * Polling Elasticsearch reader, using the "search after" capabilities of Elasticsearch.
 *
 * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after) to know the limitations.
 *
 * @property restClientBuilder supplier for the Rest client
 * @property elasticsearchPollStatement statement to execute
 * @property index index or indices to search in, as expected for the path parameter
 * @property queryParams collection of key/value pairs for the query parameters
 * @property pollDelay duration between the end of a poll and the start of the next one
 * @property jsonMapper JSON mapper to interpret the results
 * @property resultsChannelFactory factory to create the channel containing the received results sets
 * @property running running state of the reader
 * @property pollingJob instance of the background job polling data from the database
 * @property requestCancellable cancellable of the async Elasticsearch query
 *
 * @author Eric Jessé
 */
internal class ElasticsearchIterativeReader(
    private val ioCoroutineScope: CoroutineScope,
    private val ioCoroutineContext: CoroutineContext,
    private val restClientBuilder: () -> RestClient,
    private val elasticsearchPollStatement: ElasticsearchPollStatement,
    private val index: String,
    private val queryParams: Map<String, String>,
    private val pollDelay: Duration,
    private val jsonMapper: JsonMapper,
    private val resultsChannelFactory: () -> Channel<List<ObjectNode>>,
    private val meterRegistry: CampaignMeterRegistry?,
    private val eventsLogger: EventsLogger?
) : DatasourceIterativeReader<List<ObjectNode>> {

    private var running = false

    private var pollingJob: Job? = null

    private var requestCancellable: Cancellable? = null

    private var resultsChannel: Channel<List<ObjectNode>>? = null

    private val eventPrefix = "elasticsearch.poll"

    private val meterPrefix: String = "elasticsearch-poll"

    private var metersTags: Map<String, String>? = null

    private var eventTags: Map<String, String>? = null

    private var recordsByteCounter: Counter? = null

    private var receivedSuccessBytesCounter: Counter? = null

    private var receivedFailureBytesCounter: Counter? = null

    private var timeToResponse: Timer? = null

    private var successCounter: Counter? = null

    private var documentsCounter: Counter? = null

    private var failureCounter: Counter? = null

    override fun start(context: StepStartStopContext) {
        init()
        val restClient = restClientBuilder()
        running = true
        initMonitoringMetrics(context)
        pollingJob = ioCoroutineScope.launch {
            startBackgroundPolling(restClient)
        }
    }

    private fun initMonitoringMetrics(context: StepStartStopContext) {
        eventTags = context.toEventTags()
        metersTags = context.toMetersTags()
        val scenarioName = context.scenarioName
        val stepName = context.stepName

        meterRegistry?.apply {
            recordsByteCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-byte-records", metersTags!!).report {
                display(
                    format = "attempted req: %,.0f bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            receivedSuccessBytesCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-success-bytes", metersTags!!).report {
                display(
                    format = "\u2713 %,.0f byte successes",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 1,
                    Counter::count
                )
            }
            receivedFailureBytesCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-failure-bytes", metersTags!!)
            timeToResponse =
                meterRegistry.timer(scenarioName, stepName, "${meterPrefix}-time-to-response", metersTags!!)
            successCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-success", metersTags!!).report {
                display(
                    format = "\u2713 %,.0f successes",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 0,
                    Counter::count
                )
            }
            documentsCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-documents-success", metersTags!!)
            failureCounter =
                meterRegistry.counter(scenarioName, stepName, "${meterPrefix}-failure", metersTags!!).report {
                display(
                    format = "\u2716 %,.0f failures",
                    severity = ReportMessageSeverity.ERROR,
                    row = 0,
                    column = 1,
                    Counter::count
                )
            }
        }
    }

    private suspend fun startBackgroundPolling(restClient: RestClient) {
        try {
            while (running) {
                try {
                    poll(restClient)
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // Logs the error but allow next poll.
                    log.error(e) { e.message }
                } finally {
                    requestCancellable = null
                }
                if (running) {
                    delay(pollDelay.toMillis())
                }
            }
        } finally {
            resultsChannel?.close()
            resultsChannel = null
        }
    }

    @KTestable
    fun init() {
        elasticsearchPollStatement.reset()
        resultsChannel = resultsChannelFactory()
    }

    /**
     * Polls next available batch of records from Elasticsearch.
     *
     * @param restClient the active rest client
     */
    @KTestable
    private suspend fun poll(restClient: RestClient) {
        val request = Request("GET", "/${index}/_search")
        request.addParameters(queryParams)
        request.setJsonEntity(elasticsearchPollStatement.query)
        log.trace { "Polling with request $request and source ${elasticsearchPollStatement.query}" }

        val slot = ImmutableSlot<Result<Unit>>()
        val requestStart = System.nanoTime()
        recordsByteCounter?.increment(request.entity.contentLength.toDouble())
        requestCancellable = restClient.performRequestAsync(request, object : ResponseListener {
            override fun onSuccess(response: Response) {
                try {
                    timeToResponse?.record(Duration.ofNanos(System.currentTimeMillis() - requestStart))
                    val totalBytes = response.entity.contentLength.toDouble()
                    receivedSuccessBytesCounter?.increment(totalBytes)
                    successCounter?.increment(1.0)
                    eventsLogger?.info("${eventPrefix}.success.bytes", totalBytes, tags = eventTags!!)
                    processResponse(request, response, requestStart)
                    runBlocking(ioCoroutineContext) {
                        slot.set(Result.success(Unit))
                    }
                } catch (e: Exception) {
                    failureCounter?.increment(1.0)
                    log.error(e) { e.message }
                    eventsLogger?.apply {
                        info("${eventPrefix}.failure.records", 1.0, tags = eventTags!!)
                        error(
                            name = "${eventPrefix}.failure.records",
                            value = e.message,
                            tags = eventTags!!
                        )
                    }
                    runBlocking(ioCoroutineContext) {
                        slot.set(Result.failure(e))
                    }
                }
            }

            override fun onFailure(e: java.lang.Exception) {
                timeToResponse?.record(Duration.ofNanos(System.currentTimeMillis() - requestStart))
                failureCounter?.increment(1.0)
                if (e is ResponseException) {
                    val totalBytes = e.response.entity.contentLength.toDouble()
                    receivedFailureBytesCounter?.increment(totalBytes)
                    eventsLogger?.apply {
                        info("${eventPrefix}.failure.bytes", totalBytes, tags = eventTags!!)
                        info("${eventPrefix}.failure.records", 1.0, tags = eventTags!!)
                    }
                    log.error { "Received error from the server: ${EntityUtils.toString(e.response.entity)}" }
                    val exception = extractAndLogError(e)
                    runBlocking(ioCoroutineContext) {
                        slot.set(Result.failure(exception))
                    }
                } else {
                    log.error(e) { e.message }
                    eventsLogger?.apply {
                        error(
                            name = "${eventPrefix}.failure.records",
                            value = e.message,
                            tags = eventTags!!
                        )
                    }
                    runBlocking(ioCoroutineContext) {
                        slot.set(Result.failure(e))
                    }
                }
            }
        })
        slot.get().getOrThrow()
    }

    private fun processResponse(request: Request, response: Response, requestStartNanos: Long) {
        val result = EntityUtils.toByteArray(response.entity)
        val jsonTree = jsonMapper.readTree(result)
        (jsonTree.get(HITS_FIELD)?.get(HITS_FIELD) as ArrayNode?)?.let { resultsNode ->
            if (!resultsNode.isEmpty) {
                log.debug { "${resultsNode.size()} result(s) received" }
                log.trace { "Received documents from request $request with source ${elasticsearchPollStatement.query}: ${resultsNode.joinToString { it.toPrettyString() }}" }
                documentsCounter?.increment(resultsNode.size().toDouble())
                eventsLogger?.info("${eventPrefix}.success.records", resultsNode.size(), tags = eventTags!!)
                val results = resultsNode.map { it as ObjectNode }
                resultsChannel!!.trySend(results).getOrThrow()

                elasticsearchPollStatement.tieBreaker = results.last().get(SORT_FIELD) as ArrayNode
                log.trace { "The new tie-breaker is ${elasticsearchPollStatement.tieBreaker}" }
            } else {
                log.debug { "An empty result set was received" }
            }
        }
    }

    override fun stop(context: StepStartStopContext) {
        running = false
        runCatching {
            requestCancellable?.cancel()
        }
        runCatching {
            runBlocking(ioCoroutineContext) {
                pollingJob?.cancelAndJoin()
            }
        }
        pollingJob = null
        elasticsearchPollStatement.reset()

        stopMonitoringMetrics()
    }

    private fun stopMonitoringMetrics() {
        meterRegistry?.apply {
            receivedSuccessBytesCounter = null
            receivedFailureBytesCounter = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
            documentsCounter = null
            recordsByteCounter = null
        }
        eventTags = null
        metersTags = null
    }

    override suspend fun hasNext(): Boolean {
        return running
    }

    override suspend fun next(): List<ObjectNode> {
        return resultsChannel!!.receive()
    }

    companion object {

        private const val HITS_FIELD = "hits"

        private const val SORT_FIELD = "sort"

        @JvmStatic
        private val log = logger()

    }

    private fun extractAndLogError(e: Exception): ElasticsearchException {
        val res = "{\"error" + e.message?.split("error")?.get(1)
        val errorBody = res.let {
            jsonMapper.readValue(it, object : TypeReference<Map<String?, Any?>?>() {})
        }
        val error = errorBody?.get("error") as Map<*, *>
        eventsLogger?.error(
            name = error["type"].toString(),
            value = error["reason"],
            tags = eventTags!!
        )
        return ElasticsearchException("${error["type"]} : caused by ${error["reason"]}")
    }
}
