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

import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.plugins.elasticsearch.Document

/**
 * Client to save documents to Elasticsearch using the bulk API.
 *
 * @author Alex Averyanov
 */
internal interface ElasticsearchSaveQueryClient {

    /**
     * Initializes the client and connects to the Elasticsearch server.
     */
    suspend fun start(context: StepStartStopContext)

    /**
     * Indexes documents into the Elasticsearch server.
     */
    suspend fun execute(
        records: List<Document>,
        contextEventTags: Map<String, String>
    ): ElasticsearchBulkResult

    /**
     * Cleans the client and closes the connections to the Elasticsearch server.
     */
    suspend fun stop(context: StepStartStopContext)
}
