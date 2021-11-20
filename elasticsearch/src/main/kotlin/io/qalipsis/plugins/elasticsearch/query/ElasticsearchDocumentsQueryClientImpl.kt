package io.qalipsis.plugins.elasticsearch.query

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.ImmutableSlot
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Cancellable
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.ResponseListener
import org.elasticsearch.client.RestClient
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * Thread-safe client to search or send data from Elasticsearch.
 *
 * @property endpoint Elasticsearch function / endpoint to use, such as _mget, _search...
 * @property queryMonitoring the metrics for the query operation
 * @property jsonMapper JSON mapper to interpret the results
 * @property documentsExtractor closure to extract the list of JSON documents as [ObjectNode] from the response body
 * @property converter closure to convert each JSON document as [ObjectNode] into the type expected by the user
 * @property keyCounter thread-safe counter to assign a unique short-living key to each asynchronous request.
 * @property runningRequests currently running Elasticsearch requests keyed with a unique short-living key.
 *
 * @author Eric Jessé
 */
internal class ElasticsearchDocumentsQueryClientImpl<T>(
    private val ioCoroutineContext: CoroutineContext,
    private val endpoint: String,
    private val jsonMapper: JsonMapper,
    private val documentsExtractor: (JsonNode) -> List<ObjectNode>,
    private val converter: (ObjectNode) -> T
) : ElasticsearchDocumentsQueryClient<T> {

    private val keyCounter = AtomicLong()

    private val runningRequests: MutableMap<Long, Cancellable> = mutableMapOf()

    /**
     * Executes a search and returns the list of results. If an exception occurs while executing, it is thrown.
     */
    override suspend fun execute(
        restClient: RestClient, indices: List<String>, query: String,
        parameters: Map<String, String?>
    ): SearchResult<T> {

        val target = if (indices.isEmpty()) {
            ""
        } else {
            indices.joinToString(",", prefix = "/")
        }

        val request = Request("GET", "$target/$endpoint")
        request.addParameters(parameters)
        request.setJsonEntity(query)

        return withContext(ioCoroutineContext) {
            executeDocumentFetchingRequest(restClient, request)
        }
    }

    override suspend fun scroll(restClient: RestClient, scrollDuration: String, scrollId: String): SearchResult<T> {
        val request = Request("POST", "/_search/scroll")
        request.setJsonEntity("""{ "scroll" : "$scrollDuration", "scroll_id" : "$scrollId" }""")

        return withContext(ioCoroutineContext) {
            executeDocumentFetchingRequest(restClient, request)
        }
    }

    override suspend fun clearScroll(restClient: RestClient, scrollId: String) {
        val request = Request("DELETE", "/_search/scroll")
        request.setJsonEntity("""{"scroll_id" : "$scrollId" }""")
        withContext(ioCoroutineContext) {
            restClient.performRequestAsync(request, object : ResponseListener {
                override fun onSuccess(response: Response) {
                    log.trace { "Scroll context $scrollId was cleaned" }
                }

                override fun onFailure(e: Exception) {
                    log.debug(e) { "Scroll context $scrollId could not be cleaned: ${e.message}" }
                }
            })
        }
    }

    private suspend fun executeDocumentFetchingRequest(
        restClient: RestClient,
        request: Request
    ): SearchResult<T> {
        val resultsSlot = ImmutableSlot<SearchResult<T>>()
        val requestKey = keyCounter.getAndIncrement()
        val requestStart = System.nanoTime()
        runningRequests[requestKey] = restClient.performRequestAsync(request, object : ResponseListener {
            override fun onSuccess(response: Response) {
                try {
                    processDocumentFetchingResponse(response, resultsSlot, requestStart)
                } catch (e: Exception) {
                    runBlocking(ioCoroutineContext) {
                        resultsSlot.set(SearchResult(failure = e))
                    }
                }
            }

            override fun onFailure(e: Exception) {
                runBlocking(ioCoroutineContext) {
                    if (e is ResponseException) {
                        resultsSlot.set(
                            SearchResult(failure = ElasticsearchException(EntityUtils.toString(e.response.entity)))
                        )
                    } else {
                        resultsSlot.set(SearchResult(failure = e))
                    }
                }
            }
        })

        return resultsSlot.get().also {
            // Since the request is completed, it can be removed from the map.
            runningRequests.remove(requestKey)
        }
    }

    private fun processDocumentFetchingResponse(
        response: Response, resultsSlot: ImmutableSlot<SearchResult<T>>,
        requestStartNanos: Long
    ) {
        val jsonResult = jsonMapper.readTree(EntityUtils.toString(response.entity))
        val cursor = if (jsonResult.hasNonNull("_scroll_id")) {
            jsonResult.get("_scroll_id").textValue()
        } else null

        val total = if (jsonResult.hasNonNull("hits")) {
            jsonResult.get("hits").get("total").let {
                if (it.isNumber) {
                    it.intValue()
                } else {
                    it.get("value").intValue()
                }
            }
        } else {
            0
        }

        val documentsNodes = documentsExtractor(jsonResult)
        val searchResult = if (documentsNodes.isNotEmpty()) {
            log.debug { "${documentsNodes.size} results were received" }

            SearchResult(
                totalResults = if (total > 0) total else documentsNodes.size,
                results = documentsNodes.mapIndexed { index, row ->
                    ElasticsearchDocument(
                        row.get("_index").textValue(),
                        row.get("_id").textValue(),
                        index.toLong(),
                        Instant.now(),
                        tryAndLog(log) { converter(row) }
                    )
                },
                searchAfterTieBreaker = documentsNodes.last().get("sort") as ArrayNode?,
                scrollId = cursor
            )
        } else {
            log.debug { "An empty result set was received" }
            SearchResult(totalResults = total, scrollId = cursor)
        }

        runBlocking(ioCoroutineContext) {
            resultsSlot.offer(searchResult)
        }
    }

    /**
     * Cancels all the running requests.
     */
    override fun cancelAll() {
        runningRequests.values.forEach {
            runCatching {
                it.cancel()
            }
        }
        runningRequests.clear()
        keyCounter.set(0)
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }

}
