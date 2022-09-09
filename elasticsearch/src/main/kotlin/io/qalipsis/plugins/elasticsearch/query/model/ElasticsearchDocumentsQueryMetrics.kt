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


