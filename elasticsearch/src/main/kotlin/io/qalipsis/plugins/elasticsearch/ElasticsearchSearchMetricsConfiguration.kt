/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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