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

package io.qalipsis.plugins.elasticsearch.query

import io.qalipsis.api.events.EventsLogger
import io.qalipsis.plugins.elasticsearch.query.model.ElasticsearchDocumentsQueryMetrics
import org.elasticsearch.client.RestClient

/**
 *
 * @author Eric Jessé
 */
interface ElasticsearchDocumentsQueryClient<T> {

    /**
     * Executes a search and returns the list of results. If an exception occurs while executing, it is thrown.
     */
    suspend fun execute(
        restClient: RestClient, indices: List<String>, query: String,
        parameters: Map<String, String?> = emptyMap(),
        elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics?,
        eventsLogger: EventsLogger?,
        eventTags: Map<String, String>
    ): SearchResult<T>

    /**
     * Executes a scroll operation to fetch a new page.
     */
    suspend fun scroll(
        restClient: RestClient, scrollDuration: String, scrollId: String,
        elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics?,
        eventsLogger: EventsLogger?,
        eventTags: Map<String, String>
    ): SearchResult<T>

    /**
     * Clears the scrolling context.
     */
    suspend fun clearScroll(restClient: RestClient, scrollId: String)

    /**
     * Cancels all the running requests.
     */
    fun cancelAll()

    /**
     * Checks the version of the rest client.
     */
    fun init(restClient: RestClient)
}