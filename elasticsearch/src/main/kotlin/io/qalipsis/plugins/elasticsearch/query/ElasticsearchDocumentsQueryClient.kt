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
    suspend fun execute(restClient: RestClient, indices: List<String>, query: String,
                        parameters: Map<String, String?> = emptyMap(),
                        elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics?,
                        eventsLogger: EventsLogger?,
                        eventTags: Map<String, String>): SearchResult<T>

    /**
     * Executes a scroll operation to fetch a new page.
     */
    suspend fun scroll(restClient: RestClient, scrollDuration: String, scrollId: String,
                       elasticsearchDocumentsQueryMetrics: ElasticsearchDocumentsQueryMetrics?,
                       eventsLogger: EventsLogger?,
                       eventTags: Map<String, String>): SearchResult<T>

    /**
     * Clears the scrolling context.
     */
    suspend fun clearScroll(restClient: RestClient, scrollId: String)

    /**
     * Cancels all the running requests.
     */
    fun cancelAll()
}