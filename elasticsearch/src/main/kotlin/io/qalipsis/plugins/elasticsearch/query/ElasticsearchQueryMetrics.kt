package io.qalipsis.plugins.elasticsearch.query

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

/**
 * Wrapper for the meters of the Elasticsearch search operations.
 *
 * @author Eric Jessé
 */
data class ElasticsearchQueryMetrics(
        private val receivedSuccessBytesCounter: Counter? = null,
        private val receivedFailureBytesCounter: Counter? = null,
        private val documentsCounter: Counter? = null,
        private val timeToResponse: Timer? = null,
        private val successCounter: Counter? = null,
        private val failureCounter: Counter? = null,
) {

    /**
     * Records the number of received bytes when the request succeeds.
     */
    fun countReceivedSuccessBytes(size: Long) = receivedSuccessBytesCounter?.increment(size.toDouble())

    /**
     * Records the number of received bytes when the request fails.
     */
    fun countReceivedFailureBytes(size: Long) = receivedFailureBytesCounter?.increment(size.toDouble())

    /**
     * Records the number of received documents.
     */
    fun countDocuments(size: Int) = documentsCounter?.increment(size.toDouble())

    /**
     * Records the time to response in nanoseconds.
     */
    fun recordTimeToResponse(durationNanos: Long) = timeToResponse?.record(durationNanos, TimeUnit.NANOSECONDS)

    /**
     * Records a new success.
     */
    fun countSuccess() = successCounter?.increment()

    /**
     * Records a new failure.
     */
    fun countFailure() = failureCounter?.increment()
}