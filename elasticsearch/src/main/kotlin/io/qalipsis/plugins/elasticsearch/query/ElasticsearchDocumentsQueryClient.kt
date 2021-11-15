package io.qalipsis.plugins.elasticsearch.query

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
                        parameters: Map<String, String?> = emptyMap()): SearchResult<T>

    /**
     * Executes a scroll operation to fetch a new page.
     */
    suspend fun scroll(restClient: RestClient, scrollDuration: String, scrollId: String): SearchResult<T>

    /**
     * Clears the scrolling context.
     */
    suspend fun clearScroll(restClient: RestClient, scrollId: String)

    /**
     * Cancels all the running requests.
     */
    fun cancelAll()
}