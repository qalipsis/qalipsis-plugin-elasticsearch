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

package io.qalipsis.plugins.elasticsearch.monitoring.events

import io.aerisconsulting.catadioptre.KTestable
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
import io.qalipsis.plugins.elasticsearch.monitoring.ElasticsearchOperationsImpl
import io.qalipsis.plugins.elasticsearch.monitoring.PublishingMode
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.elasticsearch.client.Request
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
    private val eventsConverter: EventJsonConverter
) : AbstractBufferedEventsPublisher(
    configuration.minLevel,
    configuration.lingerPeriod,
    configuration.batchSize,
    coroutineScope
) {

    private val indexFormatter = DateTimeFormatter.ofPattern(configuration.indexDatePattern)

    private lateinit var publicationLatch: SuspendedCountLatch

    private lateinit var publicationSemaphore: Semaphore

    @KTestable
    private val elasticsearchOperations = ElasticsearchOperationsImpl()

    override fun start() {
        publicationLatch = SuspendedCountLatch(0)
        publicationSemaphore = Semaphore(configuration.publishers)
        elasticsearchOperations.buildClient(configuration)
        elasticsearchOperations.initializeTemplate(configuration, PublishingMode.EVENTS)
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
            elasticsearchOperations.close()
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
                transform = { elasticsearchOperations.createBulkItem(it, configuration.indexPrefix) }
            )

        meterRegistry.timer(
            scenarioName = "",
            stepName = "",
            name = EVENTS_CONVERSIONS_TIMER_NAME,
            tags = mapOf("publisher" to "elasticsearch")
        )
            .record(conversionStart.durationSinceNanos())
        val numberOfSentItems = values.size
        meterRegistry.counter(
            scenarioName = "",
            stepName = "",
            name = EVENTS_COUNT_TIMER_NAME,
            tags = mapOf("publisher" to "elasticsearch")
        )
            .increment(numberOfSentItems.toDouble())

        val bulkRequest = Request("POST", "_bulk")
        bulkRequest.setJsonEntity(requestBody)
        val exportStart = System.nanoTime()

        try {
            elasticsearchOperations.executeBulk(
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
                tags = mapOf("publisher" to "elasticsearch", "status" to "error")
            )
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
