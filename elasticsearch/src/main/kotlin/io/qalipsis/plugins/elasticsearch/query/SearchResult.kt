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

package io.qalipsis.plugins.elasticsearch.query

import com.fasterxml.jackson.databind.node.ArrayNode
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument

/**
 * Wrapper for a result from Elasticsearch query execution.
 *
 * @author Eric Jessé
 */
data class SearchResult<T>(
        val totalResults: Int = 0,
        val results: List<ElasticsearchDocument<T>> = emptyList(),
        val failure: Exception? = null,
        val scrollId: String? = null,
        val searchAfterTieBreaker: ArrayNode? = null
) {

    /**
     * Returns `true` if this instance represents a successful outcome.
     * In this case [isFailure] returns `false`.
     */
    val isSuccess = failure == null

    /**
     * Returns `true` if this instance represents a failed outcome.
     * In this case [isSuccess] returns `false`.
     */
    val isFailure = failure != null

    /**
     * Returns the results if this instance represents a successful outcome or throw the failure exception otherwise.
     */
    fun getOrThrow(): List<ElasticsearchDocument<T>> {
        if (isFailure) {
            throw failure!!
        }
        return results
    }
}