package io.qalipsis.plugins.elasticsearch.query

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.setProperty
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.*
import io.mockk.impl.annotations.RelaxedMockK
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.plugins.elasticsearch.query.model.ElasticsearchDocumentsQueryMetrics
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.coVerifyOnce
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.StepTestHelper
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runBlockingTest
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Instant

/**
 *
 * @author Eric Jessé
 */
@WithMockk
internal class ElasticsearchDocumentsQueryStepTest {

    @RelaxedMockK
    private lateinit var restClient: RestClient

    @RelaxedMockK
    private lateinit var restClientFactory: () -> RestClient

    @RelaxedMockK
    private lateinit var queryClient: ElasticsearchDocumentsQueryClient<String>

    private val indicesBuilder: (suspend (ctx: StepContext<*, *>, input: Int) -> List<String>) = relaxedMockk()

    private val queryParamsBuilder: (suspend (ctx: StepContext<*, *>, input: Int) -> Map<String, String?>) =
        relaxedMockk()

    private val queryBuilder: (suspend (ctx: StepContext<*, *>, input: Int) -> ObjectNode) = relaxedMockk()

    @RelaxedMockK
    private lateinit var stepStartStopContext: StepStartStopContext

    @RelaxedMockK
    private lateinit var queryNode: ObjectNode

    @RelaxedMockK
    private lateinit var meterRegistry: MeterRegistry

    @RelaxedMockK
    private lateinit var eventsLogger: EventsLogger

    private val elasticsearchDocumentsQueryMetrics = relaxedMockk<ElasticsearchDocumentsQueryMetrics>()

    private val eventTags = relaxedMockk<Map<String, String>>()

    @Test
    @Timeout(2)
    internal fun `should create the rest client at start`() = runBlockingTest {
        // given
        every { restClientFactory() } returns restClient
        val step = ElasticsearchDocumentsQueryStep(
            "", null,
            restClientFactory,
            queryClient,
            indicesBuilder,
            queryParamsBuilder,
            queryBuilder,
            false,
            meterRegistry,
            eventsLogger
        )

        // when
        step.start(stepStartStopContext)

        // then
        assertThat(step).all {
            prop("restClient").isSameAs(restClient)
        }
    }

    @Test
    @Timeout(2)
    internal fun `should close the rest client and cancel all queries at stop`() = runBlockingTest {
        // given
        val step = ElasticsearchDocumentsQueryStep(
            "", null,
            restClientFactory,
            queryClient,
            indicesBuilder,
            queryParamsBuilder,
            queryBuilder,
            false,
            meterRegistry,
            eventsLogger
        )
        step.setProperty("restClient", restClient)

        // when
        step.stop(stepStartStopContext)

        // then
        verifyOrder {
            queryClient.cancelAll()
            restClient.close()
        }
        assertThat(step).all {
            prop("restClient").isNull()
        }
    }

    @Test
    @Timeout(2)
    internal fun `should execute one search query`() = runBlockingTest {
        // given
        every { restClientFactory() } returns restClient
        val ctx = StepTestHelper.createStepContext<Int, Pair<Int, SearchResult<String>>>(input = 123)
        coEvery { indicesBuilder(refEq(ctx), eq(123)) } returns listOf("index-1", "index-2")
        coEvery { queryParamsBuilder(refEq(ctx), eq(123)) } returns mapOf("param-1" to "value-1")
        coEvery { queryBuilder(refEq(ctx), eq(123)) } returns queryNode
        every { queryNode.toString() } returns "the-query"

        val results = (1..10).mapIndexed { index, i ->
            ElasticsearchDocument("the-es-index", "_$i", index.toLong(), Instant.now(), "$i")
        }
        coEvery {
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        } returns SearchResult(
            totalResults = 100,
            results = results
        )

        val step = ElasticsearchDocumentsQueryStep(
            "", null,
            restClientFactory,
            queryClient,
            indicesBuilder,
            queryParamsBuilder,
            queryBuilder,
            false,
            meterRegistry,
            eventsLogger
        )
        step.setProperty("restClient", restClient)

        // when
        step.start(stepStartStopContext)
        step.execute(ctx)
        val (input, searchResult) = (ctx.output as Channel<Pair<Int, SearchResult<String>>>).receive()

        // then
        assertThat(input).isEqualTo(123)
        assertThat(searchResult).all {
            prop(SearchResult<String>::totalResults).isEqualTo(100)
            prop(SearchResult<String>::results).isSameAs(results)
            prop(SearchResult<String>::isFailure).isFalse()
            prop(SearchResult<String>::isSuccess).isTrue()
        }

        coVerifyOnce {
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        }
        confirmVerified(queryClient)
    }

    @Test
    @Timeout(2)
    internal fun `should page until last document when cursor is enabled`() = runBlockingTest {
        // given
        every { restClientFactory() } returns restClient
        val ctx = StepTestHelper.createStepContext<Int, Pair<Int, SearchResult<String>>>(input = 123)
        coEvery { indicesBuilder(refEq(ctx), eq(123)) } returns listOf("index-1", "index-2")
        coEvery { queryParamsBuilder(refEq(ctx), eq(123)) } returns mapOf("param-1" to "value-1",
            "scroll" to "the scroll duration")
        coEvery { queryBuilder(refEq(ctx), eq(123)) } returns queryNode
        every { queryNode.toString() } returns "the-query"

        val results = (1..10).mapIndexed { index, i ->
            ElasticsearchDocument("the-es-index", "_$i", index.toLong(), Instant.now(), "$i")
        }
        coEvery {
            queryClient.execute(any(), any(), any(), any(),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        } returns SearchResult(
            totalResults = 100,
            results = results.subList(0, 3),
            scrollId = "the scroll ID"
        )
        coEvery {
            queryClient.scroll(any(), any(), any(),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        } returns SearchResult(
            totalResults = 100,
            results = results.subList(3, 6),
            scrollId = "the scroll ID 2"
        ) andThen SearchResult(
            totalResults = 100,
            results = results.subList(6, 10)
        )

        val step = ElasticsearchDocumentsQueryStep(
            "", null,
            restClientFactory,
            queryClient,
            indicesBuilder,
            queryParamsBuilder,
            queryBuilder,
            true,
            meterRegistry,
            eventsLogger
        )
        step.setProperty("restClient", restClient)
        step.start(stepStartStopContext)
        // when
        step.execute(ctx)
        val (input, searchResult) = (ctx.output as Channel<Pair<Int, SearchResult<String>>>).receive()

        // then
        assertThat(input).isEqualTo(123)
        assertThat(searchResult).all {
            prop(SearchResult<*>::totalResults).isEqualTo(10)
            prop(SearchResult<*>::results).all {
                hasSize(10)
                (0 until 10).forEach { index ->
                    index(index).isSameAs(results[index])
                }
            }
            prop(SearchResult<*>::isFailure).isFalse()
            prop(SearchResult<*>::isSuccess).isTrue()
        }

        coVerifyOnce {
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1", "scroll" to "the scroll duration")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
            queryClient.scroll(refEq(restClient), eq("the scroll duration"), eq("the scroll ID"),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
            queryClient.scroll(refEq(restClient), eq("the scroll duration"), eq("the scroll ID 2"),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        }
        confirmVerified(queryClient)
    }

    @ExperimentalCoroutinesApi
    @Test
    @Timeout(2)
    internal fun `should return a failure when scrolling fails`() = runBlockingTest {
        // given
        every { restClientFactory() } returns restClient
        val ctx = StepTestHelper.createStepContext<Int, Pair<Int, SearchResult<String>>>(input = 123)
        coEvery { indicesBuilder(refEq(ctx), eq(123)) } returns listOf("index-1", "index-2")
        coEvery { queryParamsBuilder(refEq(ctx), eq(123)) } returns mapOf("param-1" to "value-1",
            "scroll" to "the scroll duration")
        coEvery { queryBuilder(refEq(ctx), eq(123)) } returns queryNode
        every { queryNode.toString() } returns "the-query"

        val results = (1..10).mapIndexed { index, i ->
            ElasticsearchDocument("the-es-index", "_$i", index.toLong(), Instant.now(), "$i")
        }
        coEvery {
            queryClient.execute(any(), any(), any(), any(),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        } returns SearchResult(
            totalResults = 10,
            results = results.subList(0, 5),
            scrollId = "the scroll ID"
        )
        coEvery {
            queryClient.scroll(any(), any(), any(),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        } returns SearchResult(
            failure = RuntimeException("")
        )

        val step = ElasticsearchDocumentsQueryStep(
            "", null,
            restClientFactory,
            queryClient,
            indicesBuilder,
            queryParamsBuilder,
            queryBuilder,
            true,
            meterRegistry,
            eventsLogger
        )
        step.setProperty("restClient", restClient)

        // when
        step.execute(ctx)

        // then
        assertThat(ctx).all {
            prop(StepContext<*, *>::errors).hasSize(1)
            transform { it.output as Channel<*> }.prop(Channel<*>::isEmpty).isTrue()
        }

        coVerifyOnce {
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1", "scroll" to "the scroll duration")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
            queryClient.scroll(refEq(restClient), eq("the scroll duration"), eq("the scroll ID"),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
            queryClient.clearScroll(refEq(restClient), eq("the scroll ID"))
        }
        confirmVerified(queryClient)
    }

    @Test
    @Timeout(2)
    internal fun `should page until last document when search after is enabled`() = runBlockingTest {
        // given
        every { restClientFactory() } returns restClient
        val ctx = StepTestHelper.createStepContext<Int, Pair<Int, SearchResult<String>>>(input = 123)
        coEvery { indicesBuilder(refEq(ctx), eq(123)) } returns listOf("index-1", "index-2")
        coEvery { queryParamsBuilder(refEq(ctx), eq(123)) } returns mapOf("param-1" to "value-1")
        coEvery { queryBuilder(refEq(ctx), eq(123)) } returns queryNode
        every { queryNode.toString() } returns "the-query" andThen "the-second-query" andThen "the-third-query"

        val results = (1..10).mapIndexed { index, i ->
            ElasticsearchDocument("the-es-index", "_$i", index.toLong(), Instant.now(), "$i")
        }

        val searchBreaker1: ArrayNode = relaxedMockk {
            every { isEmpty } returns false
        }
        val searchBreaker2: ArrayNode = relaxedMockk {
            every { isEmpty } returns false
        }
        val searchBreaker3: ArrayNode = relaxedMockk {
            every { isEmpty } returns false
        }
        coEvery {
            queryClient.execute(any(), any(), any(), any(),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        } returns SearchResult(
            totalResults = 5,
            results = results.subList(0, 3),
            searchAfterTieBreaker = searchBreaker1

        ) andThen SearchResult(
            totalResults = 8,
            results = results.subList(3, 6),
            searchAfterTieBreaker = searchBreaker2
        ) andThen SearchResult(
            totalResults = 10,
            results = results.subList(6, 10),
            searchAfterTieBreaker = searchBreaker3
        )

        val step = ElasticsearchDocumentsQueryStep(
            "", null,
            restClientFactory,
            queryClient,
            indicesBuilder,
            queryParamsBuilder,
            queryBuilder,
            true,
            meterRegistry,
            eventsLogger
        )
        step.setProperty("restClient", restClient)

        // when
        step.start(stepStartStopContext)
        step.execute(ctx)
        val (input, searchResult) = (ctx.output as Channel<Pair<Int, SearchResult<String>>>).receive()

        // then
        assertThat(input).isEqualTo(123)
        assertThat(searchResult).all {
            prop(SearchResult<*>::totalResults).isEqualTo(10)
            prop(SearchResult<*>::results).all {
                hasSize(10)
                (0 until 10).forEach { index ->
                    index(index).isSameAs(results[index])
                }
            }
            prop(SearchResult<*>::isFailure).isFalse()
            prop(SearchResult<*>::isSuccess).isTrue()
        }

        coVerifyOrder {
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)

            queryNode.set<ArrayNode>(eq("search_after"), refEq(searchBreaker1))
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-second-query"),
                eq(mapOf("param-1" to "value-1")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)

            queryNode.remove("search_after")
            queryNode.set<ArrayNode>(eq("search_after"), refEq(searchBreaker2))
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-third-query"),
                eq(mapOf("param-1" to "value-1")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        }
        confirmVerified(queryClient)
    }

    @ExperimentalCoroutinesApi
    @Test
    @Timeout(4)
    internal fun `should return a failure when paging next page fails`() = runBlockingTest {
        // given
        every { restClientFactory() } returns restClient
        val ctx = StepTestHelper.createStepContext<Int, Pair<Int, SearchResult<String>>>(input = 123)
        coEvery { indicesBuilder(refEq(ctx), eq(123)) } returns listOf("index-1", "index-2")
        coEvery { queryParamsBuilder(refEq(ctx), eq(123)) } returns mapOf("param-1" to "value-1")
        coEvery { queryBuilder(refEq(ctx), eq(123)) } returns queryNode
        every { queryNode.toString() } returns "the-query" andThen "the-second-query" andThen "the-third-query"

        val results = (1..10).mapIndexed { index, i ->
            ElasticsearchDocument("the-es-index", "_$i", index.toLong(), Instant.now(), "$i")
        }

        val searchBreaker: ArrayNode = relaxedMockk {
            every { isEmpty } returns false
        }
        coEvery {
            queryClient.execute(any(), any(), any(), any(),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        } returns SearchResult(
            totalResults = 5,
            results = results.subList(0, 3),
            searchAfterTieBreaker = searchBreaker
        ) andThen SearchResult(
            failure = RuntimeException("")
        )

        val step = ElasticsearchDocumentsQueryStep(
            "", null,
            restClientFactory,
            queryClient,
            indicesBuilder,
            queryParamsBuilder,
            queryBuilder,
            true,
            meterRegistry,
            eventsLogger
        )
        step.setProperty("restClient", restClient)

        // when
        step.execute(ctx)

        // then
        assertThat(ctx).all {
            prop(StepContext<*, *>::errors).hasSize(1)
            transform { it.output as Channel<*> }.prop(Channel<*>::isEmpty).isTrue()
        }

        coVerifyOnce {
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)

            queryNode.set<ArrayNode>(eq("search_after"), refEq(searchBreaker))
            queryClient.execute(refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-second-query"),
                eq(mapOf("param-1" to "value-1")),
                elasticsearchDocumentsQueryMetrics,
                eventsLogger,
                eventTags)
        }
        confirmVerified(queryClient)
    }
}
