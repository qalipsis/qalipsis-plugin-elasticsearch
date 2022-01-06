package io.qalipsis.plugins.elasticsearch.save

import java.time.Duration

/**
 * Meters of the performed query.
 *
 * @property documentsToSave total documents that are to be saved
 * @property bytesToSave total bytes saved
 * @property timeToResponse time to until the confirmation of the successful response
 * @property savedDocuments count of saved documents
 * @property failedDocuments count of not saved documents
 *
 * @author Alex Averyanov
 */
data class ElasticsearchBulkMeters(
    val documentsToSave: Int,
    val bytesToSave: Long,
    val timeToResponse: Duration,
    val savedDocuments: Int,
    val failedDocuments: Int
)