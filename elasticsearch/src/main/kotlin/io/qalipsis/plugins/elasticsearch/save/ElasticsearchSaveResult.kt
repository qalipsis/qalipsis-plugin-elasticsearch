package io.qalipsis.plugins.elasticsearch.save

import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.ElasticsearchBulkResponse

/**
 * Wrapper for the result of save records procedure in Elasticsearch.
 *
 * @property input the data to save in Elasticsearch
 * @property documentsToSave documents to be saved
 * @property responseBody response to the Elasticsearch response
 * @property meters meters of the save step
 *
 * @author Alex Averyanov
 */
data class ElasticsearchSaveResult<I>(
    val input: I,
    val documentsToSave: List<Document>,
    val responseBody: ElasticsearchBulkResponse?,
    val meters: ElasticsearchBulkMeters
)
