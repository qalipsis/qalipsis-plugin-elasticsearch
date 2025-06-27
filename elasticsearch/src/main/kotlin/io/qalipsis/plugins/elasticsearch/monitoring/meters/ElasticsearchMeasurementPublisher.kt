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

package io.qalipsis.plugins.elasticsearch.monitoring.meters

import io.aerisconsulting.catadioptre.KTestable
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.MeasurementPublisher
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.elasticsearch.monitoring.ElasticsearchOperationsImpl
import io.qalipsis.plugins.elasticsearch.monitoring.PublishingMode
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.elasticsearch.client.Request
import java.time.Clock
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * Implementation of measurement publisher to export meters to elasticsearch.
 *
 * @author Francisca Eze
 */
@Requires(beans = [ElasticsearchMeasurementConfiguration::class])
internal class ElasticsearchMeasurementPublisher(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineScope: CoroutineScope,
    private val configuration: ElasticsearchMeasurementConfiguration,
) : MeasurementPublisher {

    private val indexFormatter = DateTimeFormatter.ofPattern(configuration.indexDatePattern)

    private var publicationLatch: SuspendedCountLatch = SuspendedCountLatch(0)

    private var publicationSemaphore: Semaphore = Semaphore(configuration.publishers)

    @KTestable
    private val elasticsearchOperations = ElasticsearchOperationsImpl()

    /**
     * Initializes Elasticsearch and make it available for exporting of data.
     */
    override suspend fun init() {
        publicationLatch = SuspendedCountLatch(0)
        publicationSemaphore = Semaphore(configuration.publishers)
        elasticsearchOperations.buildClient(configuration)
        elasticsearchOperations.initializeTemplate(configuration, PublishingMode.METERS)
    }

    override suspend fun publish(meters: Collection<MeterSnapshot>) {
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

    private suspend fun performPublish(meterSnapshots: Collection<MeterSnapshot>) {
        logger.debug { "Sending ${meterSnapshots.size} meters to Elasticsearch" }
        // Convert the data for a bulk post.
        val requestBody = meterSnapshots
            .map(this@ElasticsearchMeasurementPublisher::metersToJsonConverter)
            .joinToString(
                separator = "\n",
                postfix = "\n",
                transform = { elasticsearchOperations.createBulkItem(it, configuration.indexPrefix) }
            )
        val numberOfSentItems = meterSnapshots.size
        val bulkRequest = Request("POST", "_bulk")
        bulkRequest.setJsonEntity(requestBody)
        val exportStart = System.nanoTime()

        try {
            elasticsearchOperations.executeBulk(
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
    private fun metersToJsonConverter(meterSnapshot: MeterSnapshot): Pair<String, String> {
        val stringBuilder = StringBuilder()
        val timestamp = indexFormatter.format(ZonedDateTime.ofInstant(meterSnapshot.timestamp, Clock.systemUTC().zone))
        val meterId = meterSnapshot.meterId
        val type = meterId.type.value.lowercase()
        val tags = meterId.tags
        stringBuilder.append("{\"")
            .append("@timestamp")
            .append("\":\"")
            .append(meterSnapshot.timestamp.toEpochMilli())
            .append('"')
            .append(",\"name\":\"")
            .append(meterId.meterName.format())
            .append('"')
            .append(",\"@type\":\"")
            .append(type)
            .append('"')
        if (tags.isNotEmpty()) {
            stringBuilder.append(",")
            val jsonTags = tags.entries.joinToString(",") { "\"${it.key}\":\"${it.value}\"" }
            stringBuilder.append(""""tags":{$jsonTags}""")
        }
        val measurementJson = meterSnapshot.measurements.joinToString(separator = ",") {
            if (it is DistributionMeasurementMetric) {
                """{"statistic":"${it.statistic.value.lowercase()}","percentile":${it.observationPoint},"value":${it.value}}"""
            } else {
                """{"statistic":"${it.statistic.value.lowercase()}","value":${it.value}}"""
            }
        }

        stringBuilder.append(""","metrics":[$measurementJson]""")
        stringBuilder.append('}')

        return timestamp to stringBuilder.toString()
    }

    override suspend fun stop() {
        logger.debug { "Stopping the meter publication of meters" }
        publicationLatch.await()
        logger.debug { "Closing the Elasticsearch client" }
        tryAndLogOrNull(logger) {
            elasticsearchOperations.close()
        }
        logger.debug { "The meters logger was stopped" }
    }

    /**
     * Replace spaces to a hyphen(-) as well as convert to a lowercase. This is to provide uniformity among name indexes.
     */
    private fun String.format() = this.replace(" ", "-").lowercase()

    companion object {

        @JvmStatic
        private val logger = logger()
    }

}