package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import io.qalipsis.api.sync.Latch
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
 * @property queryMetrics metrics for the monitoring
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
    private val meterRegistry: MeterRegistry?,
    private val eventsLogger: EventsLogger?
) : DatasourceIterativeReader<List<ObjectNode>> {

    private var running = false

    private var pollingJob: Job? = null

    private var requestCancellable: Cancellable? = null

    private var resultsChannel: Channel<List<ObjectNode>>? = null

    private val eventPrefix = "elasticsearch.poll"

    private val meterPrefix: String = "elasticsearch-poll"

    private var eventTags: Map<String, String>? = null

    private var meterTags: Tags? = null

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
        meterTags = context.toMetersTags()
        eventTags = context.toEventTags()

        meterRegistry?.apply {
            receivedSuccessBytesCounter = meterRegistry.counter("${meterPrefix}-success-bytes", meterTags)
            receivedFailureBytesCounter = meterRegistry.counter("${meterPrefix}-failure-bytes", meterTags)
            timeToResponse = meterRegistry.timer("${meterPrefix}-ttr", meterTags)
            successCounter = meterRegistry.counter("${meterPrefix}-success", meterTags)
            documentsCounter = meterRegistry.counter("${meterPrefix}-documents-success", meterTags)
            failureCounter = meterRegistry.counter("${meterPrefix}-failure", meterTags)
        }
    }

    private suspend fun startBackgroundPolling(restClient: RestClient) {
        try {
            while (running) {
                try {
                    poll(restClient)
                    if (running) {
                        delay(pollDelay.toMillis())
                    }
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

        val latch = Latch(true)
        val requestStart = System.nanoTime()
        requestCancellable = restClient.performRequestAsync(request, object : ResponseListener {
            override fun onSuccess(response: Response) {
                try {
                    timeToResponse?.record(Duration.ofNanos(System.currentTimeMillis() - requestStart))
                    val totalBytes = response.entity.contentLength.toDouble()
                    receivedSuccessBytesCounter?.increment(totalBytes)
                    successCounter?.increment(1.0)
                    eventsLogger?.info("${eventPrefix}.success.bytes", totalBytes, tags = eventTags!!)
                    processResponse(request, response, requestStart)
                } catch (e: Exception) {
                    failureCounter?.increment(1.0)
                    eventsLogger?.apply {
                        info("${eventPrefix}.failure.records", 1.0, tags = eventTags!!)
                    }
                    log.error(e) { e.message }
                }
                runBlocking(ioCoroutineContext) {
                    latch.release()
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
                } else {
                    log.error(e) { e.message }
                }
                runBlocking(ioCoroutineContext) {
                    latch.release()
                }
            }
        })
        latch.await()
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
            remove(receivedSuccessBytesCounter)
            remove(receivedFailureBytesCounter)
            remove(timeToResponse)
            remove(successCounter)
            remove(failureCounter)
            remove(documentsCounter)
            receivedSuccessBytesCounter = null
            receivedFailureBytesCounter = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
            documentsCounter = null
        }
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
}
