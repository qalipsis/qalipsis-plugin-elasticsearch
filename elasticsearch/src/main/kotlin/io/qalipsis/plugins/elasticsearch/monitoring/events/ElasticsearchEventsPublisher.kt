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

package io.qalipsis.plugins.elasticsearch.monitoring.events

import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.events.AbstractBufferedEventsPublisher
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventJsonConverter
import io.qalipsis.api.lang.durationSinceNanos
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.elasticsearch.monitoring.ElasticsearchPublisherImpl
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient
import java.time.Clock
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
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
    private val meterRegistry: CampaignMeterRegistry,
    private val eventsConverter: EventJsonConverter,
    private val abstractElasticsearchPublisher: ElasticsearchPublisherImpl,
) : AbstractBufferedEventsPublisher(
    configuration.minLevel,
    configuration.lingerPeriod,
    configuration.batchSize,
    coroutineScope
) {

    private val indexFormatter = DateTimeFormatter.ofPattern(configuration.indexDatePattern)

    private lateinit var restClient: RestClient

    private lateinit var publicationLatch: SuspendedCountLatch

    private lateinit var publicationSemaphore: Semaphore

    override fun start() {
        publicationLatch = SuspendedCountLatch(0)
        publicationSemaphore = Semaphore(configuration.publishers)
        abstractElasticsearchPublisher.buildClient(configuration)
        abstractElasticsearchPublisher.initializeTemplate(configuration, "events")
        super.start()
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
                transform = abstractElasticsearchPublisher::createBulkItem
            )

        meterRegistry.timer(
            scenarioName = "",
            stepName = "",
            name = EVENTS_CONVERSIONS_TIMER_NAME,
            tags = mapOf("publisher" to "elasticsearch"))
            .record(conversionStart.durationSinceNanos())
        val numberOfSentItems = values.size
        meterRegistry.counter(
            scenarioName = "",
            stepName = "",
            name = EVENTS_COUNT_TIMER_NAME,
            tags = mapOf("publisher" to "elasticsearch"))
            .increment(numberOfSentItems.toDouble())

        val bulkRequest = Request("POST", "_bulk")
        bulkRequest.setJsonEntity(requestBody)
        val exportStart = System.nanoTime()

        try {
            abstractElasticsearchPublisher.executeBulk(
                bulkRequest,
                exportStart,
                numberOfSentItems,
                meterRegistry,
                coroutineContext,
                "events"
            )
        } catch (e: Exception) {
            // TODO Reprocess 3 times when an exception is received.
            meterRegistry.timer(
                scenarioName = "",
                stepName = "",
                name = EVENTS_EXPORT_TIMER_NAME,
                tags = mapOf("publisher" to "elasticsearch", "status" to "error"))
                .record(exportStart.durationSinceNanos())
            log.error(e) { e.message }
        }
    }

    private fun createPairOfDateToDocument(event: Event) =
        indexFormatter.format(
            ZonedDateTime.ofInstant(
                event.timestamp,
                Clock.systemUTC().zone
            )
        ) to eventsConverter.convert(event)

    companion object {

        private const val EVENTS_CONVERSIONS_TIMER_NAME = "elasticsearch.events.conversion"

        private const val EVENTS_COUNT_TIMER_NAME = "elasticsearch.events.converted"

        private const val EVENTS_EXPORT_TIMER_NAME = "elasticsearch.events.export"

        @JvmStatic
        private val log = logger()
    }

}
