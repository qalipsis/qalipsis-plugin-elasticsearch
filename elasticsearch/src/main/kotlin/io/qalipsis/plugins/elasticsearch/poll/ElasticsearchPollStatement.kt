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

package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.node.ArrayNode

/**
 * Elasticsearch statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Eric Jessé
 */
internal interface ElasticsearchPollStatement {

    /**
     * The values used to select the next batch of data.
     *
     * See [Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after) for more details.
     */
    var tieBreaker: ArrayNode?

    /**
     * Builds the JSON query given the past executions and tie-breaker value.
     */
    val query: String

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()
}