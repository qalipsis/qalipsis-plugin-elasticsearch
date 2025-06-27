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
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode

internal class ElasticsearchPollStatementImpl(
        private var jsonBuilder: () -> ObjectNode
) : ElasticsearchPollStatement {

    private var json = jsonBuilder()

    init {
        val sortNode = json.get("sort")
        when (sortNode) {
            is TextNode -> {
                if (sortNode.textValue().isBlank()) {
                    throw IllegalArgumentException("The provided query has no sort clause")
                }
            }
            is ArrayNode -> {
                if (sortNode.isEmpty) {
                    throw IllegalArgumentException("The provided query has no sort clause")
                }
            }
            else -> {
                throw IllegalArgumentException("The provided query has no valid sort clause")
            }
        }
    }

    override var tieBreaker: ArrayNode? = null
        set(value) {
            field = value
            insertTieBreakerClause()
        }

    private fun insertTieBreakerClause() {
        json.remove("search_after")
        if (tieBreaker != null && !tieBreaker!!.isEmpty) {
            json.set<ArrayNode>("search_after", tieBreaker!!)
        }
    }

    override val query: String
        get() = json.toString()

    override fun reset() {
        tieBreaker = null
        json = jsonBuilder()
    }
}
