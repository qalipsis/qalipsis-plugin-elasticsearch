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

package io.qalipsis.plugins.elasticsearch.save

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.Slot
import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.ElasticsearchBulkResponse
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.plugins.elasticsearch.ElasticsearchSaveException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Cancellable
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.ResponseListener
import org.elasticsearch.client.RestClient
import java.time.Duration
import java.util.Random
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern


/**
 * Implementation of [ElasticsearchSaveQueryClient].
 * Client to query to Elasticsearch.
 *
 * @author Alex Averianov
 */
internal class ElasticsearchSaveQueryClientImpl(
    private val ioCoroutineScope: CoroutineScope,
    private val clientBuilder: () -> RestClient,
    private val jsonMapper: JsonMapper,
    private val keepElasticsearchBulkResponse: Boolean,
    private var eventsLogger: EventsLogger?,
    private val meterRegistry: MeterRegistry?
) : ElasticsearchSaveQueryClient {
    private var requestCancellable: Cancellable? = null

    private val random = Random()

    private lateinit var client: RestClient

    private val eventPrefix = "elasticsearch.save"

    private val meterPrefix: String = "elasticsearch-save"

    private var documentsCount: Counter? = null

    private var timeToResponseTimer: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null

    private var savedBytesCounter: Counter? = null

    private var failureBytesCounter: Counter? = null

    private var majorVersionIsSevenOrMore = false

    override suspend fun start(context: StepStartStopContext) {
        client = clientBuilder()
        init()
        meterRegistry?.apply {
            val meterTags = context.toMetersTags()
            documentsCount = counter("$meterPrefix-received-documents", meterTags)
            timeToResponseTimer = timer("$meterPrefix-time-to-response", meterTags)
            successCounter = counter("$meterPrefix-successes", meterTags)
            failureCounter = counter("$meterPrefix-failures", meterTags)
            savedBytesCounter = counter("$meterPrefix-success-bytes", meterTags)
            failureBytesCounter = counter("$meterPrefix-failure-bytes", meterTags)
        }
    }

    private fun init() {
        log.debug { "Checking the version of ES" }

        val versionTree =
            jsonMapper.readTree(EntityUtils.toByteArray(client.performRequest(Request("GET", "/")).entity))
        val version = (versionTree.get("version")?.get("number") as TextNode).textValue()
        majorVersionIsSevenOrMore = version.substringBefore(".").toInt() >= 7
        log.debug { "Using Elasticsearch $version" }
    }

    override suspend fun execute(
        records: List<Document>,
        contextEventTags: Map<String, String>
    ): ElasticsearchBulkResult {
        val requestBody = records
            .joinToString(
                separator = "\n",
                postfix = "\n",
            ) { createBulkItem(it) }

        val request = Request("POST", "/_bulk")
        request.setJsonEntity(requestBody)
        val response = send(client, request, records, contextEventTags)
        if (response.responseBody?.responseBody?.contains(ERROR_RESPONSE_BODY_SIGNATURE) == true) {
            val res = jsonMapper.readValue(
                response.responseBody.responseBody,
                object : TypeReference<Map<String?, Any?>?>() {})
            extractErrors(res!!, contextEventTags)
        }

        return response
    }

    /**
     * Creates a unique indexation item for the bulk request.
     *
     * See also [the official Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html).
     */
    private fun createBulkItem(document: Document): String {
        val documentId = document.id ?: UUID(random.nextLong(), random.nextLong())
        val type = if (majorVersionIsSevenOrMore) "" else """"_type":"${document.type}","""
        return """{"index":{"_index":"${document.index}",$type"_id":"$documentId"}}
            ${document.source}""".trimIndent()
    }

    /**
     * Executes a Bulk request to Elasticsearch.
     *
     * @param restClient the active rest client
     */
    @KTestable
    private suspend fun send(
        restClient: RestClient,
        request: Request,
        documents: List<Document>,
        contextEventTags: Map<String, String>
    ): ElasticsearchBulkResult {
        val numberOfSentItems: Int = documents.size
        eventsLogger?.debug(
            "$eventPrefix.saving.documents",
            numberOfSentItems,
            tags = contextEventTags
        )
        documentsCount?.increment(numberOfSentItems.toDouble())
        var timeToResponse: Duration
        val result = Slot<Result<ElasticsearchBulkResult>>()
        val requestStart = System.nanoTime()
        requestCancellable = restClient.performRequestAsync(request, object : ResponseListener {
            override fun onSuccess(response: Response) {
                try {
                    val timeToResponseNano = System.nanoTime() - requestStart
                    timeToResponse = Duration.ofNanos(timeToResponseNano - requestStart)
                    eventsLogger?.info(
                        "$eventPrefix.time-to-response",
                        timeToResponse,
                        tags = contextEventTags
                    )
                    timeToResponseTimer?.record(timeToResponseNano, TimeUnit.NANOSECONDS)
                    val totalBytes = response.entity.contentLength
                    eventsLogger?.info("${eventPrefix}.success.bytes", totalBytes, tags = contextEventTags)
                    val response = processResponse(
                        request,
                        response,
                        numberOfSentItems,
                        totalBytes,
                        timeToResponse,
                        contextEventTags
                    )
                    ioCoroutineScope.launch {
                        result.set(Result.success(response))
                    }
                } catch (e: ElasticsearchException) {
                    ioCoroutineScope.launch {
                        result.set(Result.failure(e))
                    }
                } catch (e: Exception) {
                    failureCounter?.increment(numberOfSentItems.toDouble())
                    eventsLogger?.apply {
                        warn("${eventPrefix}.failure.documents", numberOfSentItems, tags = contextEventTags)
                        error("${eventPrefix}.failure.records", e.message, tags = contextEventTags)
                    }
                    ioCoroutineScope.launch {
                        result.set(Result.failure(e))
                    }
                }
            }

            override fun onFailure(e: java.lang.Exception) {
                val timeToResponseNano = System.nanoTime() - requestStart
                timeToResponse = Duration.ofNanos(timeToResponseNano - requestStart)
                eventsLogger?.info(
                    "$eventPrefix.time-to-response",
                    timeToResponse,
                    tags = contextEventTags
                )
                timeToResponseTimer?.record(timeToResponseNano, TimeUnit.NANOSECONDS)
                failureCounter?.increment(numberOfSentItems.toDouble())
                if (e is ResponseException) {
                    val totalBytes = e.response.entity.contentLength.toDouble()
                    eventsLogger?.apply {
                        warn("${eventPrefix}.failure.bytes", totalBytes, tags = contextEventTags)
                        warn("${eventPrefix}.failure.documents", numberOfSentItems, tags = contextEventTags)
                    }
                    failureBytesCounter?.increment(totalBytes)
                    failureCounter?.increment(numberOfSentItems.toDouble())
                    log.debug { "Received error from the server: ${EntityUtils.toString(e.response.entity)}" }
                    val response = ElasticsearchBulkResult(
                        ElasticsearchBulkResponse(
                            httpStatus = e.response.statusLine.statusCode,
                            responseBody = e.response.toString()
                        ),
                        ElasticsearchBulkMeters(
                            timeToResponse = timeToResponse, savedDocuments = 0,
                            failedDocuments = numberOfSentItems, bytesToSave = 0, documentsToSave = numberOfSentItems
                        )
                    )
                    ioCoroutineScope.launch {
                        val exception = ElasticsearchSaveException("Received error from the server", response)
                        result.set(Result.failure(exception))
                    }
                } else {
                    ioCoroutineScope.launch {
                        result.set(Result.failure(e))
                    }
                }

            }
        })
        return result.get().getOrThrow()
    }

    private fun processResponse(
        bulkRequest: Request, response: Response, numberOfSentItems: Int,
        totalBytes: Long, timeToResponse: Duration, contextEventTags: Map<String, String>
    ): ElasticsearchBulkResult {
        val responseBody = EntityUtils.toString(response.entity)
        if (responseBody.contains(ERROR_RESPONSE_BODY_SIGNATURE)) {
            val numberOfCreatedItems = countCreatedItems(responseBody)
            eventsLogger?.apply {
                info("${eventPrefix}.success.documents", numberOfCreatedItems, tags = contextEventTags)
                warn(
                    "${eventPrefix}.failure.documents",
                    numberOfSentItems - numberOfCreatedItems,
                    tags = contextEventTags
                )
            }
            successCounter?.increment(numberOfCreatedItems.toDouble())
            failureCounter?.increment((numberOfSentItems - numberOfCreatedItems).toDouble())
            val errors = jsonMapper.readTree(responseBody)
                .withArray<ObjectNode>("items")
                .asSequence()
                .map { it["index"] as ObjectNode } // Reads the index operation.
                .filterNot { it["status"].asInt(400) == 201 } // Finds the ones with a status != 201.
                .onEach {
                    val error = it["error"]
                    eventsLogger?.error(
                        "${eventPrefix}.failure.document",
                        "Index: ${it["_index"].asText()}, ID: ${it["_id"]} ${error["reason"]}",
                        tags = contextEventTags
                    )
                }
                .map { "Document ${it["_id"].asText()}: ${it["error"]}" }
                .toList()

            if (errors.isNotEmpty()) {
                log.debug {
                    "Failed to save documents into Elasticsearch: $responseBody${
                        errors.joinToString(
                            "\n\t\t",
                            prefix = "\n\t\t"
                        )
                    }"
                }
                throw ElasticsearchException("Failed to save documents into Elasticsearch")
            }

            log.debug { "Failed events payload: ${bulkRequest.entity}" }
            return if (keepElasticsearchBulkResponse) {
                ElasticsearchBulkResult(
                    ElasticsearchBulkResponse(httpStatus = response.statusLine.statusCode, responseBody = responseBody),
                    ElasticsearchBulkMeters(
                        timeToResponse = timeToResponse,
                        savedDocuments = numberOfCreatedItems,
                        failedDocuments = numberOfSentItems - numberOfCreatedItems,
                        bytesToSave = 0,
                        documentsToSave = numberOfSentItems
                    )
                )
            } else {
                ElasticsearchBulkResult(
                    null,
                    ElasticsearchBulkMeters(
                        timeToResponse = timeToResponse,
                        savedDocuments = numberOfCreatedItems,
                        failedDocuments = numberOfSentItems - numberOfCreatedItems,
                        bytesToSave = 0,
                        documentsToSave = numberOfSentItems
                    )
                )
            }
        } else {
            log.trace { "Successfully saved $numberOfSentItems events to Elasticsearch" }
            log.trace { "Successfully saved $totalBytes bytes to Elasticsearch" }
            successCounter?.increment(numberOfSentItems.toDouble())
            savedBytesCounter?.increment(totalBytes.toDouble())
            return if (keepElasticsearchBulkResponse) {
                ElasticsearchBulkResult(
                    ElasticsearchBulkResponse(httpStatus = response.statusLine.statusCode, responseBody = responseBody),
                    ElasticsearchBulkMeters(
                        timeToResponse = timeToResponse, savedDocuments = numberOfSentItems,
                        failedDocuments = 0, bytesToSave = totalBytes, documentsToSave = numberOfSentItems
                    )
                )
            } else {
                ElasticsearchBulkResult(
                    null,
                    ElasticsearchBulkMeters(
                        timeToResponse = timeToResponse, savedDocuments = numberOfSentItems,
                        failedDocuments = 0, bytesToSave = totalBytes, documentsToSave = numberOfSentItems
                    )
                )
            }
        }
    }

    @KTestable
    private fun countCreatedItems(responseBody: String): Int {
        val matcher = STATUS_CREATED_PATTERN.matcher(responseBody)
        var count = 0
        while (matcher.find()) {
            count++
        }
        return count
    }

    override suspend fun stop(context: StepStartStopContext) {
        runCatching {
            requestCancellable?.cancel()
        }
        meterRegistry?.apply {
            remove(documentsCount!!)
            remove(timeToResponseTimer!!)
            remove(successCounter!!)
            remove(failureCounter!!)
            remove(savedBytesCounter!!)
            remove(failureBytesCounter!!)
            documentsCount = null
            timeToResponseTimer = null
            successCounter = null
            failureCounter = null
            savedBytesCounter = null
            failureBytesCounter = null
        }
        tryAndLog(log) {
            client.close()
        }
    }

    companion object {

        const val ERROR_RESPONSE_BODY_SIGNATURE = "\"errors\":true"

        @JvmStatic
        private val STATUS_CREATED_PATTERN = Pattern.compile("\"status\":201")

        @JvmStatic
        private val log = logger()

    }

    private fun extractErrors(res: Map<String?, Any?>, contextEventTags: Map<String, String>) {
        val items = res["items"] as List<Any?>
        val first = items.first() as Map<*, *>

        val firstIndex = first["index"] as Map<*, *>
        val errorObject = firstIndex["error"] as Map<*, *>
        val cause = errorObject["caused_by"] as Map<*, *>
        eventsLogger?.error(
            name = errorObject["type"].toString(),
            value = errorObject["reason"],
            tags = contextEventTags
        )
        throw ElasticsearchException(
            """
                ${errorObject["type"]} : caused by ${cause["reason"]}
            """.trimIndent()
        )
    }

}
