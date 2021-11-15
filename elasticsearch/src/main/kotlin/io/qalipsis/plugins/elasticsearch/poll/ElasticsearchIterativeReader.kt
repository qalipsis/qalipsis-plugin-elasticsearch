package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import io.qalipsis.api.sync.Latch
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchQueryMetrics
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
    private val queryMetrics: ElasticsearchQueryMetrics,
    private val jsonMapper: JsonMapper,
    private val resultsChannelFactory: () -> Channel<List<ObjectNode>>
) : DatasourceIterativeReader<List<ObjectNode>> {

    private var running = false

    private var pollingJob: Job? = null

    private var requestCancellable: Cancellable? = null

    private var resultsChannel: Channel<List<ObjectNode>>? = null

    override fun start(context: StepStartStopContext) {
        init()
        val restClient = restClientBuilder()
        running = true
        pollingJob = ioCoroutineScope.launch {
            startBackgroundPolling(restClient)
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
                    queryMetrics.countFailure()
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
    private fun init() {
        elasticsearchPollStatement.reset()
        resultsChannel = resultsChannelFactory()
    }

    /**
     * Polls next available batch of records from Elasticsearch.
     *
     * @param restClient the active rest client
     */
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
                    processResponse(request, response, requestStart)
                    queryMetrics.countSuccess()
                } catch (e: Exception) {
                    queryMetrics.countFailure()
                    log.error(e) { e.message }
                }
                runBlocking(ioCoroutineContext) {
                    latch.release()
                }
            }

            override fun onFailure(e: java.lang.Exception) {
                queryMetrics.countFailure()
                if (e is ResponseException) {
                    queryMetrics.countReceivedFailureBytes(e.response.entity.contentLength)
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
        queryMetrics.recordTimeToResponse(System.nanoTime() - requestStartNanos)
        queryMetrics.countReceivedSuccessBytes(response.entity.contentLength)
        val result = EntityUtils.toByteArray(response.entity)
        val jsonTree = jsonMapper.readTree(result)
        (jsonTree.get(HITS_FIELD)?.get(HITS_FIELD) as ArrayNode?)?.let { resultsNode ->
            if (!resultsNode.isEmpty) {
                log.debug { "${resultsNode.size()} result(s) received" }
                log.trace { "Received documents from request $request with source ${elasticsearchPollStatement.query}: ${resultsNode.joinToString { it.toPrettyString() }}" }
                queryMetrics.countDocuments(resultsNode.size())
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
