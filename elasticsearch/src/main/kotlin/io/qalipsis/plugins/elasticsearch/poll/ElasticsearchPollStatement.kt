package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.node.ArrayNode

/**
 * Elasticsearch statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Eric Jessé
 */
internal interface ElasticsearchPollStatement {

    /**
     * The values used to select the next batch of data.
     *
     * See [Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after) for more details.
     */
    var tieBreaker: ArrayNode?

    /**
     * Builds the JSON query given the past executions and tie-breaker value.
     */
    val query: String

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()
}