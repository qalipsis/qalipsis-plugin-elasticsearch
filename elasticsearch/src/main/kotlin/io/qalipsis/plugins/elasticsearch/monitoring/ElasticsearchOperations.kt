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

package io.qalipsis.plugins.elasticsearch.monitoring

import io.qalipsis.api.meters.CampaignMeterRegistry
import org.elasticsearch.client.Request
import kotlin.coroutines.CoroutineContext

/**
 * Handles initialization of elasticsearch templates, as well as exporting data into elasticsearch.
 *
 * @author Francisca Eze
 */
internal interface ElasticsearchOperations {

    /**
     * Initializes Elasticsearch and keeps it ready for publishing of data.
     */
    fun buildClient(configuration: MonitoringConfiguration)

    /**
     * Initializes the templates, taking into account if types are supported or not in the used ES version.
     * This operation is synchronous to make the full starting process fail if something gets wrong.
     *
     */
    fun initializeTemplate(configuration: MonitoringConfiguration, publishingMode: PublishingMode)

    /**
     * Creates a unique indexation item for the bulk request.
     *
     * See also [the official Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html).
     */
    fun createBulkItem(formattedDateToDocument: Pair<String, String>, indexPrefix: String): String

    /**
     * Handles export of data into Elasticsearch.
     */
    suspend fun executeBulk(
        bulkRequest: Request,
        exportStart: Long,
        numberOfSentItems: Int,
        meterRegistry: CampaignMeterRegistry?,
        coroutineContext: CoroutineContext,
        monitoringType: String,
    )

    /**
     * Copied from [io.micrometer.elastic.ElasticMeterRegistry.countCreatedItems].
     */
    fun countCreatedItems(responseBody: String): Int

    /**
     * Closes all the open connections.
     */
    fun close()
}