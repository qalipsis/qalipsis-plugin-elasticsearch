/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.elasticsearch.save

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.sync.Slot
import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.ElasticsearchBulkResponse
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import io.qalipsis.plugins.elasticsearch.ElasticsearchSaveException
import io.qalipsis.plugins.elasticsearch.ElasticsearchUtility.checkElasticsearchVersionIsGreaterThanSeven
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
    private val meterRegistry: CampaignMeterRegistry?
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
            val metersTags = context.toMetersTags()
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            documentsCount = counter(scenarioName, stepName, "$meterPrefix-received-documents", metersTags).report {
                display(
                    format = "attempted req %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            timeToResponseTimer = timer(scenarioName, stepName, "$meterPrefix-time-to-response", metersTags)
            successCounter = counter(scenarioName, stepName, "$meterPrefix-successes", metersTags).report {
                display(
                    format = "\u2713 %,.0f successes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 2,
                    Counter::count
                )
            }
            failureCounter = counter(scenarioName, stepName, "$meterPrefix-failures", metersTags).report {
                display(
                    format = "\u2716 %,.0f failures",
                    severity = ReportMessageSeverity.ERROR,
                    row = 0,
                    column = 4,
                    Counter::count
                )
            }
            savedBytesCounter = counter(scenarioName, stepName, "$meterPrefix-success-bytes", metersTags).report {
                display(
                    format = "\u2713 %,.0f byte successes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 3,
                    Counter::count
                )
            }
            failureBytesCounter = counter(scenarioName, stepName, "$meterPrefix-failure-bytes", metersTags)
        }
    }

    private fun init() {
        log.debug { "Checking the version of Elasticsearch" }
        val version = checkElasticsearchVersionIsGreaterThanSeven(jsonMapper, client)
        majorVersionIsSevenOrMore = version >= 7
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
                        "${eventPrefix}.failure.documents",
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
