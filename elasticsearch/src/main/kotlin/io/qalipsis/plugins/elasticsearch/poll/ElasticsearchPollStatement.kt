/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
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