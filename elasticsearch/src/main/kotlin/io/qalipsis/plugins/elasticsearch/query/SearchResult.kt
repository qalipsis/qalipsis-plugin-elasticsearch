package io.qalipsis.plugins.elasticsearch.query

import com.fasterxml.jackson.databind.node.ArrayNode
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument

/**
 * Wrapper for a result from Elasticsearch query execution.
 *
 * @author Eric Jessé
 */
data class SearchResult<T>(
        val totalResults: Int = 0,
        val results: List<ElasticsearchDocument<T>> = emptyList(),
        val failure: Exception? = null,
        val scrollId: String? = null,
        val searchAfterTieBreaker: ArrayNode? = null
) {

    /**
     * Returns `true` if this instance represents a successful outcome.
     * In this case [isFailure] returns `false`.
     */
    val isSuccess = failure == null

    /**
     * Returns `true` if this instance represents a failed outcome.
     * In this case [isSuccess] returns `false`.
     */
    val isFailure = failure != null

    /**
     * Returns the results if this instance represents a successful outcome or throw the failure exception otherwise.
     */
    fun getOrThrow(): List<ElasticsearchDocument<T>> {
        if (isFailure) {
            throw failure!!
        }
        return results
    }
}