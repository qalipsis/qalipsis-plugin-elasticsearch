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

package io.qalipsis.plugins.elasticsearch.events

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.events.AbstractBufferedEventsPublisher
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventJsonConverter
import io.qalipsis.api.lang.durationSinceNanos
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.ImmutableSlot
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseListener
import org.elasticsearch.client.RestClient
import java.time.Clock
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Random
import java.util.UUID
import java.util.regex.Pattern
import kotlin.coroutines.CoroutineContext

/**
 * Implementation of [io.qalipsis.api.events.EventsLogger] for Elasticsearch.
 *
 * @author Eric Jessé
 */
@Singleton
@Requires(beans = [ElasticsearchEventsConfiguration::class])
internal class ElasticsearchEventsPublisher(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineScope: CoroutineScope,
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineContext: CoroutineContext,
    private val configuration: ElasticsearchEventsConfiguration,
    private val meterRegistry: MeterRegistry,
    private val eventsConverter: EventJsonConverter
) : AbstractBufferedEventsPublisher(
    configuration.minLevel,
    configuration.lingerPeriod,
    configuration.batchSize,
    coroutineScope
) {

    private val indexFormatter = DateTimeFormatter.ofPattern(configuration.indexDatePattern)

    private val jsonMapper: ObjectMapper = ObjectMapper()

    private val random = Random()

    private lateinit var restClient: RestClient

    private var majorVersionIsSevenOrMore = false

    private lateinit var publicationLatch: SuspendedCountLatch

    private lateinit var publicationSemaphore: Semaphore

    override fun start() {
        publicationLatch = SuspendedCountLatch(0)
        publicationSemaphore = Semaphore(configuration.publishers)
        buildClient()
        initializeTemplate()
        super.start()
    }

    /**
     * Creates a brand new client related to the configuration.
     *
     * @see ElasticsearchEventsConfiguration
     */
    private fun buildClient() {
        val builder = RestClient.builder(*configuration.urls.map { HttpHost.create(it) }.toTypedArray())
        builder.setPathPrefix(configuration.pathPrefix)
        configuration.proxy?.let {
            builder.setHttpClientConfigCallback { asyncBuilder ->
                if (!configuration.username.isNullOrBlank() && !configuration.password.isNullOrBlank()) {
                    asyncBuilder.setDefaultCredentialsProvider(
                        BasicCredentialsProvider().also {
                            it.setCredentials(
                                AuthScope.ANY,
                                UsernamePasswordCredentials(configuration.username, configuration.password)
                            )
                        }
                    )
                }
                if (!configuration.proxy.isNullOrBlank()) {
                    asyncBuilder.setProxy(HttpHost.create(configuration.proxy))
                }
                asyncBuilder
            }
        }
        restClient = builder.build()
    }

    /**
     * Initializes the templates, taking into account if types are supported or not in the used ES version.
     * This operation is synchronous to make the full starting process fail if something gets wrong.
     *
     */
    private fun initializeTemplate() {
        log.debug { "Checking the version of ES" }
        val versionTree =
            jsonMapper.readTree(EntityUtils.toByteArray(restClient.performRequest(Request("GET", "/")).entity))
        val version = (versionTree.get("version")?.get("number") as TextNode).textValue()
        majorVersionIsSevenOrMore = version.substringBefore(".").toInt() >= 7
        log.debug { "Using Elasticsearch $version" }

        val templateResource =
            if (majorVersionIsSevenOrMore) "events/index-template-from-7.json" else "events/index-template-before-7.json"
        val jsonTemplate = jsonMapper.readTree(this::class.java.classLoader.getResource(templateResource)) as ObjectNode
        val templateName = "qalipsis-events"
        jsonTemplate.put("index_patterns", "${configuration.indexPrefix}-*")
        (jsonTemplate["aliases"] as ObjectNode).putObject(configuration.indexPrefix)
        (jsonTemplate["settings"] as ObjectNode).apply {
            put("number_of_shards", configuration.shards)
            put("number_of_replicas", configuration.replicas)
            (this["index"] as ObjectNode).put("refresh_interval", configuration.refreshInterval)
        }

        (if (majorVersionIsSevenOrMore) {
            (jsonTemplate["mappings"] as ObjectNode)
        } else {
            ((jsonTemplate["mappings"] as ObjectNode)["_doc"] as ObjectNode)
        }["_source"] as ObjectNode).put("enabled", configuration.storeSource)

        val putTemplateRequest = Request("PUT", "_template/$templateName")
        putTemplateRequest.setJsonEntity(jsonTemplate.toString())
        val templateCreationResponse = EntityUtils.toString(restClient.performRequest(putTemplateRequest).entity)
        if (templateCreationResponse.contains(ERROR_RESPONSE_BODY_SIGNATURE)) {
            log.error { "Failed to create or update the index template ${templateName} in Elasticsearch" }
        } else {
            log.debug { "Successfully created or updated the index template ${templateName} in Elasticsearch" }
        }
    }

    override fun stop() {
        log.debug { "Stopping the events logger with ${buffer.size} events in the buffer" }
        super.stop()
        runBlocking(coroutineContext) {
            log.debug { "Waiting for ${publicationLatch.get()} publication jobs to be completed" }
            publicationLatch.await()
        }
        log.debug { "Closing the Elasticsearch client" }
        tryAndLogOrNull(log) {
            restClient.close()
        }
        log.debug { "The events logger was stopped" }
    }

    override suspend fun publish(values: List<Event>) {
        publicationLatch.increment()
        coroutineScope.launch {
            try {
                publicationSemaphore.withPermit {
                    performPublish(values)
                }
            } finally {
                publicationLatch.decrement()
            }
        }
    }

    private suspend fun performPublish(values: List<Event>) {
        log.debug { "Sending ${values.size} events to Elasticsearch" }
        val conversionStart = System.nanoTime()
        // Convert the data for a bulk post.
        val requestBody = values
            .map(this@ElasticsearchEventsPublisher::createPairOfDateToDocument)
            .joinToString(
                separator = "\n",
                postfix = "\n",
                transform = this@ElasticsearchEventsPublisher::createBulkItem
            )

        meterRegistry.timer(EVENTS_CONVERSIONS_TIMER_NAME, "publisher", "elasticsearch")
            .record(conversionStart.durationSinceNanos())
        val numberOfSentItems = values.size
        meterRegistry.counter(EVENTS_COUNT_TIMER_NAME, "publisher", "elasticsearch")
            .increment(numberOfSentItems.toDouble())

        val bulkRequest = Request("POST", "_bulk")
        bulkRequest.setJsonEntity(requestBody)
        val exportStart = System.nanoTime()

        try {
            executeBulk(bulkRequest, exportStart, numberOfSentItems)
        } catch (e: Exception) {
            // TODO Reprocess 3 times when an exception is received.
            meterRegistry.timer(EVENTS_EXPORT_TIMER_NAME, "publisher", "elasticsearch", "status", "error")
                .record(exportStart.durationSinceNanos())
            log.error(e) { e.message }
        }
    }

    private suspend fun executeBulk(bulkRequest: Request, exportStart: Long, numberOfSentItems: Int) {
        val slot = ImmutableSlot<Result<Unit>>()
        restClient.performRequestAsync(bulkRequest, object : ResponseListener {
            override fun onSuccess(response: Response) {
                val exportEnd = System.nanoTime()
                val responseBody = EntityUtils.toString(response.entity)
                if (responseBody.contains(ERROR_RESPONSE_BODY_SIGNATURE)) {
                    meterRegistry.timer(EVENTS_EXPORT_TIMER_NAME, "publisher", "elasticsearch", "status", "failure")
                        .record(Duration.ofNanos(exportEnd - exportStart))
                    val numberOfCreatedItems = countCreatedItems(responseBody)
                    val errors = jsonMapper.readTree(responseBody)
                        .withArray<ObjectNode>("items")
                        .asSequence()
                        .map { it["index"] as ObjectNode } // Reads the index operation.
                        .filterNot { it["status"].asInt(400) == 201 } // Finds the ones with a status != 201.
                        .map { "Document ${it["_id"].asText()}: ${it["error"]}" }

                    log.error {
                        "Failed to send events to Elasticsearch (sent $numberOfSentItems events but only created $numberOfCreatedItems): $responseBody\n${
                            errors.joinToString("\n\t\t", prefix = "\t\t")
                        }"
                    }
                    log.debug { "Failed events payload: ${bulkRequest.entity}" }
                    runBlocking(coroutineContext) {
                        slot.set(Result.failure(ElasticsearchException(responseBody)))
                    }
                } else {
                    meterRegistry.timer(EVENTS_EXPORT_TIMER_NAME, "publisher", "elasticsearch", "status", "success")
                        .record(Duration.ofNanos(exportEnd - exportStart))
                    log.debug { "Successfully sent $numberOfSentItems events to Elasticsearch" }
                    runBlocking(coroutineContext) {
                        slot.set(Result.success(Unit))
                    }
                }
                log.trace { "onSuccess totally processed" }
            }

            override fun onFailure(exception: Exception) {
                runBlocking(coroutineContext) {
                    slot.set(Result.failure(exception))
                }
                log.trace { "onFailure totally processed" }
            }
        })
        slot.get().getOrThrow()
    }

    /**
     * Creates a unique indexation item for the bulk request.
     *
     * See also [the official Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html).
     */
    private fun createBulkItem(formattedDateToDocument: Pair<String, String>): String {
        val uuid = UUID(random.nextLong(), random.nextLong())
        val type = if (majorVersionIsSevenOrMore) "" else """"_type":"$DOCUMENT_TYPE","""
        return """{"index":{"_index":"${configuration.indexPrefix}-${formattedDateToDocument.first}",$type"_id":"$uuid"}}
${formattedDateToDocument.second}""".trimIndent()
    }

    private fun createPairOfDateToDocument(event: Event) =
        indexFormatter.format(
            ZonedDateTime.ofInstant(
                event.timestamp,
                Clock.systemUTC().zone
            )
        ) to eventsConverter.convert(event)

    /**
     * Copied from [io.micrometer.elastic.ElasticMeterRegistry.countCreatedItems].
     */
    @KTestable
    private fun countCreatedItems(responseBody: String): Int {
        val matcher = STATUS_CREATED_PATTERN.matcher(responseBody)
        var count = 0
        while (matcher.find()) {
            count++
        }
        return count
    }

    companion object {

        private const val DOCUMENT_TYPE = "_doc"

        private const val EVENTS_CONVERSIONS_TIMER_NAME = "elasticsearch.events.conversion"

        private const val EVENTS_COUNT_TIMER_NAME = "elasticsearch.events.converted"

        private const val EVENTS_EXPORT_TIMER_NAME = "elasticsearch.events.export"

        @JvmStatic
        private val ERROR_RESPONSE_BODY_SIGNATURE = "\"errors\":true"

        @JvmStatic
        private val STATUS_CREATED_PATTERN = Pattern.compile("\"status\":201")

        @JvmStatic
        private val log = logger()
    }

}
