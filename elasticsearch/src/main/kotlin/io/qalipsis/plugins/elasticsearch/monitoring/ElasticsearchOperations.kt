/*
 * Copyright 2024 AERIS IT Solutions GmbH
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
}