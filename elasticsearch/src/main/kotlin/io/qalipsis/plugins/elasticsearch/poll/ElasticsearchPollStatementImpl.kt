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
