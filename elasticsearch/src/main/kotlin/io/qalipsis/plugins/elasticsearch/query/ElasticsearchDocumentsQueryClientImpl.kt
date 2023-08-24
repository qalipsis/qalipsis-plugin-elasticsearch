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

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.ImmutableSlot
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.plugins.elasticsearch.ElasticsearchUtility.checkElasticsearchVersionIsGreaterThanSeven
import io.qalipsis.plugins.elasticsearch.query.model.ElasticsearchDocumentsQueryMetrics
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Cancellable
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.ResponseListener
import org.elasticsearch.client.RestClient
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * Thread-safe client to search or send data from Elasticsearch.
 *
 * @property ioCoroutineContext local coroutine context...
 * @property endpoint Elasticsearch function / endpoint to use, such as _mget, _search...
 * @property jsonMapper JSON mapper to interpret the results
 * @property documentsExtractor closure to extract the list of JSON documents as [ObjectNode] from the response body
 * @property converter closure to convert each JSON document as [ObjectNode] into the type expected by the user
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

    private val eventPrefix = "elasticsearch.query"

    private var majorVersionIsSevenOrMore = false

    private val pattern = "\"_type\"\\s*:\\s*\"\\w+\"\\s*,".toRegex()

    private val typeConversion =
        { query: String -> if (majorVersionIsSevenOrMore && query.contains(pattern)) query.replace(pattern, "") else query }

    /**
     * Checks the version of the restClient
     */
    override fun init(restClient: RestClient) {
        log.debug { "Checking the version of Elasticsearch" }
        val version = checkElasticsearchVersionIsGreaterThanSeven(jsonMapper, restClient)
        majorVersionIsSevenOrMore = version >= 7
        log.debug { "Using Elasticsearch $version" }
    }

    /**
     * Executes a search and returns the list of results. If an exception occurs while executing, it is thrown.
     */
    override suspend fun execute(
        restClient: RestClient, indices: List<String>, query: String,
        parameters: Map<String, String?>,
        elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics?,
        eventsLogger: EventsLogger?,
        eventTags: Map<String, String>
    ): SearchResult<T> {

        val target = if (indices.isEmpty()) {
            ""
        } else {
            indices.joinToString(",", prefix = "/")
        }

        val request = Request("GET", "$target/$endpoint")
        request.addParameters(parameters)
        request.setJsonEntity(typeConversion(query))

        val result = withContext(ioCoroutineContext) {
            executeDocumentFetchingRequest(
                restClient,
                request,
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags
            )
        }
        if (result.failure != null) {
            throw result.failure
        }
        return result
    }

    override suspend fun scroll(
        restClient: RestClient, scrollDuration: String, scrollId: String,
        elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics?,
        eventsLogger: EventsLogger?,
        eventTags: Map<String, String>
    ): SearchResult<T> {
        val request = Request("POST", "/_search/scroll")
        request.setJsonEntity("""{ "scroll" : "$scrollDuration", "scroll_id" : "$scrollId" }""")

        return withContext(ioCoroutineContext) {
            executeDocumentFetchingRequest(
                restClient,
                request,
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags
            )
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
        request: Request,
        elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics?,
        eventsLogger: EventsLogger?,
        eventTags: Map<String, String>
    ): SearchResult<T> {
        val resultsSlot = ImmutableSlot<SearchResult<T>>()
        val requestKey = keyCounter.getAndIncrement()
        val requestStart = System.nanoTime()
        elasticsearchDocumentsQueryMetrics?.recordsCounter?.increment(1.0)
        runningRequests[requestKey] = restClient.performRequestAsync(request, object : ResponseListener {
            override fun onSuccess(response: Response) {
                try {
                    processDocumentFetchingResponse(
                        response,
                        resultsSlot,
                        requestStart,
                        elasticsearchDocumentsQueryMetrics,
                        eventsLogger,
                        eventTags
                    )
                    elasticsearchDocumentsQueryMetrics?.successCounter?.increment(1.0)
                    eventsLogger?.info("${eventPrefix}.success.times", 1, tags = eventTags)
                } catch (e: Exception) {
                    elasticsearchDocumentsQueryMetrics?.failureCounter?.increment(1.0)
                    eventsLogger?.apply {
                        warn("${eventPrefix}.failure.times", 1, tags = eventTags)
                        error("${eventPrefix}.failure.records", e.message, tags = eventTags)
                    }
                    runBlocking(ioCoroutineContext) {
                        resultsSlot.set(SearchResult(failure = e))
                    }
                }
            }

            override fun onFailure(e: Exception) {
                elasticsearchDocumentsQueryMetrics?.failureCounter?.increment(1.0)
                eventsLogger?.warn("${eventPrefix}.failure.times", 1, tags = eventTags)
                runBlocking(ioCoroutineContext) {
                    if (e is ResponseException) {
                        elasticsearchDocumentsQueryMetrics?.receivedFailureBytesCounter?.increment(e.response.entity.contentLength.toDouble())
                        eventsLogger?.warn(
                            "${eventPrefix}.failure.bytes",
                            e.response.entity.contentLength,
                            tags = eventTags
                        )
                        val exception = extractAndLogError(e, eventsLogger, eventTags)
                        resultsSlot.set(
                            SearchResult(failure = exception)
                        )
                    } else {
                        eventsLogger?.apply {
                            error(
                                name = "${eventPrefix}.failure.records",
                                value = e.message,
                                tags = eventTags
                            )
                        }
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
        requestStartNanos: Long,
        elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics?,
        eventsLogger: EventsLogger?,
        eventTags: Map<String, String>
    ) {
        val timeToResponse = Duration.ofNanos(System.nanoTime() - requestStartNanos)
        val totalBytes = response.entity.contentLength
        elasticsearchDocumentsQueryMetrics?.timeToResponse?.record(timeToResponse)
        elasticsearchDocumentsQueryMetrics?.receivedSuccessBytesCounter?.increment(totalBytes.toDouble())
        eventsLogger?.info("${eventPrefix}.success.time-to-response", timeToResponse, tags = eventTags)
        eventsLogger?.info("${eventPrefix}.success.received-bytes", totalBytes, tags = eventTags)

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
            elasticsearchDocumentsQueryMetrics?.documentsCounter?.increment(documentsNodes.size.toDouble())
            eventsLogger?.info("${eventPrefix}.success.records", documentsNodes.size, tags = eventTags)
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

    private fun extractAndLogError(
        e: Exception,
        eventsLogger: EventsLogger?,
        eventTags: Map<String, String>?
    ): ElasticsearchException {
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
