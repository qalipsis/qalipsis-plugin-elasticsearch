package io.qalipsis.plugins.elasticsearch.save

import io.qalipsis.plugins.elasticsearch.ElasticsearchBulkResponse

/**
 * Result of the execution of a bulk step.
 *
 * @property responseBody response of the performed query
 * @property meters meters of the performed query
 *
 * @author Alex Averyanov
 */
data class ElasticsearchBulkResult (
    val responseBody: ElasticsearchBulkResponse?,
    val meters: ElasticsearchBulkMeters
)