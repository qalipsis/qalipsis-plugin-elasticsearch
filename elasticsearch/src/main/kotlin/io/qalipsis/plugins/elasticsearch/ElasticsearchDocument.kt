package io.qalipsis.plugins.elasticsearch

import java.time.Instant

/**
 * Wrapper for a fetched Elasticsearch document.
 *
 * @author Eric Jessé
 */
data class ElasticsearchDocument<O>(
        val index: String,
        val id: String,
        val ordinal: Long,
        val receivingInstant: Instant,
        val value: O
)