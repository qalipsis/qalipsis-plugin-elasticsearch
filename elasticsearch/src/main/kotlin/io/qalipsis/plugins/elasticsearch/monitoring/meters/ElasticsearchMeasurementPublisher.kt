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

package io.qalipsis.plugins.elasticsearch.monitoring.meters

import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.MeasurementPublisher
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.elasticsearch.monitoring.ElasticsearchPublisher
import jakarta.inject.Named
import java.time.Clock
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient

/**
 * Implementation of measurement publisher to export meters to elasticsearch.
 *
 * @author Francisca Eze
 */
@Requires(beans = [ElasticsearchMeasurementConfiguration::class])
internal class ElasticsearchMeasurementPublisher(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineScope: CoroutineScope,
    private val configuration: ElasticsearchMeasurementConfiguration,
    private val elasticsearchPublisher: ElasticsearchPublisher,
) : MeasurementPublisher {

    private val indexFormatter = DateTimeFormatter.ofPattern(configuration.indexDatePattern)

    private lateinit var restClient: RestClient

    private var publicationLatch: SuspendedCountLatch = SuspendedCountLatch(0)

    private var publicationSemaphore: Semaphore = Semaphore(configuration.publishers)

    private var prefix: String = if (configuration.prefix.isNotEmpty()) "${configuration.prefix}." else ""


    /**
     * Initializes elasticsearch and keeps it ready for publishing of data.
     */
    override suspend fun init() {
        publicationLatch = SuspendedCountLatch(0)
        publicationSemaphore = Semaphore(configuration.publishers)
        elasticsearchPublisher.buildClient(configuration)
        elasticsearchPublisher.initializeTemplate(configuration, "meters")
    }

    override suspend fun publish(meters: Collection<MeterSnapshot<*>>) {
        publicationLatch.increment()
        coroutineScope.launch {
            try {
                publicationSemaphore.withPermit {
                    performPublish(meters)
                }
            } finally {
                publicationLatch.decrement()
            }
        }
    }

    private suspend fun performPublish(meterSnapshots: Collection<MeterSnapshot<*>>) {
        logger.debug { "Sending ${meterSnapshots.size} meters to Elasticsearch" }
        // Convert the data for a bulk post.
        val requestBody = meterSnapshots
            .map(this@ElasticsearchMeasurementPublisher::metersToJsonConverter)
            .joinToString(
                separator = "\n",
                postfix = "\n",
                transform = elasticsearchPublisher::createBulkItem
            )
        val numberOfSentItems = meterSnapshots.size
        val bulkRequest = Request("POST", "_bulk")
        bulkRequest.setJsonEntity(requestBody)
        val exportStart = System.nanoTime()

        try {
            elasticsearchPublisher.executeBulk(
                bulkRequest,
                exportStart,
                numberOfSentItems,
                null,
                coroutineScope.coroutineContext,
                "meters"
            )
        } catch (e: Exception) {
            // TODO Reprocess 3 times when an exception is received.
            logger.error(e) { e.message }
        }
    }

    /**
     * Converts a collection of [MeterSnapshot]s to json format.
     */
    private fun metersToJsonConverter(meterSnapshot: MeterSnapshot<*>): Pair<String, String> {
        val stringBuilder = StringBuilder()
        val timestamp = indexFormatter.format(ZonedDateTime.ofInstant(meterSnapshot.timestamp, Clock.systemUTC().zone))
        val meterId = meterSnapshot.meter.id
        val name =
            "$prefix${meterId.campaignKey.replaceSpaces()}.${meterId.scenarioName.replaceSpaces()}.${meterId.stepName.replaceSpaces()}.${meterId.meterName.replaceSpaces()}".lowercase()
        val type = meterId.type.value.lowercase()
        val tags = meterId.tags
        stringBuilder.append("{\"")
            .append("@timestamp")
            .append("\":\"")
            .append(meterSnapshot.timestamp.truncatedTo(ChronoUnit.MILLIS).toEpochMilli())
            .append('"')
            .append(",\"name\":\"")
            .append(name)
            .append('"')
            .append(",\"type\":\"")
            .append(type)
            .append('"')
        if (tags.isNotEmpty()) {
            stringBuilder.append(",")
            val jsonTags = tags.entries.joinToString(",") { "\"${it.key}\":\"${it.value}\"" }
            stringBuilder.append(""""tags":{$jsonTags}""")
        }
        val measurementJson = meterSnapshot.measurements.joinToString(
            separator = ",",
        ) {
            if (it is DistributionMeasurementMetric) {
                val formattedObservationPoint = "%.1f".format(it.observationPoint)
                """{"statistic":"${it.statistic.value.lowercase()}_$formattedObservationPoint","value":"${it.value}"}"""
            } else {
                """{"statistic":"${it.statistic.value.lowercase()}","value":"${it.value}"}"""
            }
        }

        stringBuilder.append(""","metrics":[$measurementJson]""")
        stringBuilder.append('}')

        return timestamp to stringBuilder.toString()
    }

    override suspend fun stop() {
        logger.debug { "Stopping the meter publication of meters" }
        runBlocking(coroutineScope.coroutineContext) {
            logger.debug { "Waiting for ${publicationLatch.get()} publication jobs to be completed" }
            publicationLatch.await()
        }
        logger.debug { "Closing the Elasticsearch client" }
        tryAndLogOrNull(logger) {
            restClient.close()
        }
        logger.debug { "The meters logger was stopped" }
    }

    /**
     * Replace spaces to a hyphen(-) as well as convert to a lowercase. This is to provide uniformity among name indexes.
     */
    private fun String.replaceSpaces() = this.replace(" ", "-").lowercase()

    companion object {

        @JvmStatic
        private val logger = logger()
    }

}