package io.qalipsis.plugins.elasticsearch.query.model

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer

/**
 * @author rklymenko
 */
data class ElasticsearchDocumentsQueryMetrics(
    val receivedSuccessBytesCounter: Counter,
    val receivedFailureBytesCounter: Counter,
    val timeToResponse: Timer,
    val successCounter: Counter,
    val failureCounter: Counter,
    val documentsCounter: Counter
)


