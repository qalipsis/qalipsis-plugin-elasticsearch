package io.qalipsis.plugins.elasticsearch

/**
 * Entity to save in Elasticsearch.
 *
 * @property httpStatus Status of request for a BULK API
 * @property responseBody  ResponseBody as the result of executing save
 *
 * @author Alex Averyanov
 */
data class ElasticsearchBulkResponse (
    val httpStatus: Int,
    val responseBody: String
)