package io.qalipsis.plugins.elasticsearch.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.isNull
import assertk.assertions.prop
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.skyscreamer.jsonassert.JSONAssert

/**
 *
 * @author Eric Jessé
 */
internal class ElasticsearchPollStatementImplTest {

    val mapper = JsonMapper()

    @Test
    internal fun `should fail when there is no sort clause`() {
        val node = mapper.readTree("""
                {
                "query": {
                    "term": {
                        "action": "IN"
                    }
                }
            }
            """.trimMargin()) as ObjectNode

        assertThrows<IllegalArgumentException> {
            ElasticsearchPollStatementImpl { node }
        }
    }

    @Test
    internal fun `should fail when the sort clause is a blank text`() {
        val node = mapper.readTree("""
                {
                "query": {
                    "term": {
                        "action": "IN"
                    }
                },
                "sort":"  "
            }
            """.trimMargin()) as ObjectNode

        assertThrows<IllegalArgumentException> {
            ElasticsearchPollStatementImpl { node }
        }
    }

    @Test
    internal fun `should fail when the sort clause is a empty array`() {
        val node = mapper.readTree("""
            {
            "query": {
                "term": {
                    "action": "IN"
                }
            },
            "sort":[]
        }
        """.trimMargin()) as ObjectNode

        assertThrows<IllegalArgumentException> {
            ElasticsearchPollStatementImpl { node }
        }
    }


    @Test
    internal fun `should add the tie-breaker clause when the sort clause is an array`() {
        // given
        val nodeFactory = ObjectMapper().nodeFactory
        val baseQuery = """
            {
                "query": {
                    "term": {
                        "action": "IN"
                    }
                },
                "sort":[
                    "timestamp",
                    {"username":"desc"}
                ]
            }
            """.trimMargin()
        val node = mapper.readTree(baseQuery) as ObjectNode
        val esPollStatement = ElasticsearchPollStatementImpl { node }

        // then
        JSONAssert.assertEquals(esPollStatement.query, baseQuery, false)
        assertThat(esPollStatement).all {
            prop(ElasticsearchPollStatementImpl::tieBreaker).isNull()
        }

        // when
        esPollStatement.tieBreaker = null

        // then
        JSONAssert.assertEquals(esPollStatement.query, baseQuery, false)
        assertThat(esPollStatement).all {
            prop(ElasticsearchPollStatementImpl::tieBreaker).isNull()
        }

        // when
        var tieBreaker = ArrayNode(nodeFactory)
        tieBreaker.add("Value 1")
        tieBreaker.add(true)
        tieBreaker.add(12)
        esPollStatement.tieBreaker = tieBreaker

        // then
        JSONAssert.assertEquals(esPollStatement.query, """
            {
                "query": {
                    "term": {
                        "action": "IN"
                    }
                },
                "search_after": ["Value 1", true, 12],
                "sort":[
                    "timestamp",
                    {"username":"desc"}
                ]
            }
            """.trimMargin(), false)

        // when
        tieBreaker = ArrayNode(nodeFactory)
        tieBreaker.add("Value 2")
        tieBreaker.add(false)
        tieBreaker.add(20)
        esPollStatement.tieBreaker = tieBreaker

        // then
        JSONAssert.assertEquals(esPollStatement.query, """
            {
                "query": {
                    "term": {
                        "action": "IN"
                    }
                },
                "search_after": ["Value 2", false, 20],
                "sort":[
                    "timestamp",
                    {"username":"desc"}
                ]
            }
            """.trimMargin(), false)

        // when
        esPollStatement.reset()

        // then
        JSONAssert.assertEquals(esPollStatement.query, baseQuery, false)
        assertThat(esPollStatement).all {
            prop(ElasticsearchPollStatementImpl::tieBreaker).isNull()
        }

        // Verifies that the reset is idem-potent.
        // when
        esPollStatement.reset()

        // then
        JSONAssert.assertEquals(esPollStatement.query, baseQuery, false)
        assertThat(esPollStatement).all {
            prop(ElasticsearchPollStatementImpl::tieBreaker).isNull()
        }
    }
}