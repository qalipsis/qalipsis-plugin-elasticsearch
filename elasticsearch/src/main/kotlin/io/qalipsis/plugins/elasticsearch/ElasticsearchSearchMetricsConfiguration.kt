package io.qalipsis.plugins.elasticsearch

import io.qalipsis.api.annotations.Spec

/**
 * Configuration of the metrics to record for the Elasticsearch search step.
 *
 * @property receivedSuccessBytesCount when true, records the number of received bytes when the request succeeds, as received in the HTTP response.
 * @property receivedFailureBytesCount when true, records the number of received bytes when the request fails, as received in the HTTP response.
 * @property receivedDocumentsCount when true, records the number of received documents.
 * @property timeToResponse when true, records the time from the request execution to a successful response.
 * @property successCount when true, records the number of success (successful request and response processing).
 * @property failureCount when true, records the number of failures (failed request or response processing).
 *
 * @author Eric Jessé
 */
@Spec
data class ElasticsearchSearchMetricsConfiguration(
        var receivedSuccessBytesCount: Boolean = false,
        var receivedFailureBytesCount: Boolean = false,
        var receivedDocumentsCount: Boolean = false,
        var timeToResponse: Boolean = false,
        var successCount: Boolean = false,
        var failureCount: Boolean = false
) {
    fun all() {
        receivedSuccessBytesCount = true
        receivedFailureBytesCount = true
        receivedDocumentsCount = true
        timeToResponse = true
        successCount = true
        failureCount = true
    }
}