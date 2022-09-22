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