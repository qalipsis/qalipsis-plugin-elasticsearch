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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import assertk.assertions.prop
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.aerisconsulting.catadioptre.setProperty
import io.mockk.coEvery
import io.mockk.coVerifyOrder
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.verifyOrder
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.coVerifyOnce
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.StepTestHelper
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Instant

/**
 *
 * @author Eric Jessé
 */
@WithMockk
internal class ElasticsearchDocumentsQueryStepTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

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
    private lateinit var meterRegistry: CampaignMeterRegistry

    @RelaxedMockK
    private lateinit var eventsLogger: EventsLogger

    private val recordsCounter = relaxedMockk<Counter>()

    private val receivedSuccessBytesCounter = relaxedMockk<Counter>()

    private val successCounter = relaxedMockk<Counter>()

    private val failureCounter = relaxedMockk<Counter>()

    @Test
    @Timeout(2)
    internal fun `should create the rest client at start`() = testDispatcherProvider.runTest {
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
        val tags = emptyMap<String, String>()
        every { stepStartStopContext.toMetersTags() } returns tags
        every { stepStartStopContext.scenarioName } returns "scenario-name"
        every { stepStartStopContext.stepName } returns "step-name"
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-records", refEq(tags)) } returns recordsCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-success-bytes", refEq(tags)) } returns successCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-failure", refEq(tags)) } returns failureCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-success", refEq(tags)) } returns failureCounter
        every { successCounter.report(any()) } returns successCounter
        every { recordsCounter.report(any()) } returns recordsCounter
        every { receivedSuccessBytesCounter.report(any()) } returns receivedSuccessBytesCounter
        every { failureCounter.report(any()) } returns failureCounter

        // when
        step.start(stepStartStopContext)

        // then
        assertThat(step).all {
            prop("restClient").isSameAs(restClient)
        }
    }

    @Test
    @Timeout(2)
    internal fun `should close the rest client and cancel all queries at stop`() = testDispatcherProvider.runTest {
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
    internal fun `should execute one search query`() = testDispatcherProvider.runTest {
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
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1")),
                any(),
                eventsLogger,
                any()
            )
        } returns SearchResult(
            totalResults = 100,
            results = results
        )
        val tags = emptyMap<String, String>()
        every { stepStartStopContext.toMetersTags() } returns tags
        every { stepStartStopContext.scenarioName } returns "scenario-name"
        every { stepStartStopContext.stepName } returns "step-name"
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-records", refEq(tags)) } returns recordsCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-success-bytes", refEq(tags)) } returns successCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-failure", refEq(tags)) } returns failureCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-success", refEq(tags)) } returns failureCounter
        every { successCounter.report(any()) } returns successCounter
        every { recordsCounter.report(any()) } returns recordsCounter
        every { receivedSuccessBytesCounter.report(any()) } returns receivedSuccessBytesCounter
        every { failureCounter.report(any()) } returns failureCounter

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
        val (input, searchResult) = (ctx.output as Channel<StepContext.StepOutputRecord<Pair<Int, SearchResult<String>>>>).receive().value

        // then
        assertThat(input).isEqualTo(123)
        assertThat(searchResult).all {
            prop(SearchResult<String>::totalResults).isEqualTo(100)
            prop(SearchResult<String>::results).isSameAs(results)
            prop(SearchResult<String>::isFailure).isFalse()
            prop(SearchResult<String>::isSuccess).isTrue()
        }

        coVerifyOnce {
            queryClient.init(restClient)
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1")),
                any(),
                eventsLogger,
                any()
            )
        }
        confirmVerified(queryClient)
    }

    @Test
    @Timeout(2)
    internal fun `should page until last document when cursor is enabled`() = testDispatcherProvider.runTest {
        // given
        every { restClientFactory() } returns restClient
        val ctx = StepTestHelper.createStepContext<Int, Pair<Int, SearchResult<String>>>(input = 123)
        coEvery { indicesBuilder(refEq(ctx), eq(123)) } returns listOf("index-1", "index-2")
        coEvery { queryParamsBuilder(refEq(ctx), eq(123)) } returns mapOf(
            "param-1" to "value-1",
            "scroll" to "the scroll duration"
        )
        coEvery { queryBuilder(refEq(ctx), eq(123)) } returns queryNode
        every { queryNode.toString() } returns "the-query"

        val results = (1..10).mapIndexed { index, i ->
            ElasticsearchDocument("the-es-index", "_$i", index.toLong(), Instant.now(), "$i")
        }
        coEvery {
            queryClient.execute(
                any(), any(), any(), any(),
                any(),
                any(),
                any()
            )
        } returns SearchResult(
            totalResults = 100,
            results = results.subList(0, 3),
            scrollId = "the scroll ID"
        )
        coEvery {
            queryClient.scroll(
                any(), any(), any(),
                any(),
                any(),
                any()
            )
        } returns SearchResult(
            totalResults = 100,
            results = results.subList(3, 6),
            scrollId = "the scroll ID 2"
        ) andThen SearchResult(
            totalResults = 100,
            results = results.subList(6, 10)
        )
        val tags = emptyMap<String, String>()
        every { stepStartStopContext.toMetersTags() } returns tags
        every { stepStartStopContext.scenarioName } returns "scenario-name"
        every { stepStartStopContext.stepName } returns "step-name"
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-records", refEq(tags)) } returns recordsCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-success-bytes", refEq(tags)) } returns successCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-failure", refEq(tags)) } returns failureCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-success", refEq(tags)) } returns failureCounter
        every { successCounter.report(any()) } returns successCounter
        every { recordsCounter.report(any()) } returns recordsCounter
        every { receivedSuccessBytesCounter.report(any()) } returns receivedSuccessBytesCounter
        every { failureCounter.report(any()) } returns failureCounter


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
        val (input, searchResult) = (ctx.output as Channel<StepContext.StepOutputRecord<Pair<Int, SearchResult<String>>>>).receive().value

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
            queryClient.init(restClient)
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1", "scroll" to "the scroll duration")),
                any(),
                eventsLogger,
                any()
            )
            queryClient.scroll(
                refEq(restClient), eq("the scroll duration"), eq("the scroll ID"),
                any(),
                eventsLogger,
                any()
            )
            queryClient.scroll(
                refEq(restClient), eq("the scroll duration"), eq("the scroll ID 2"),
                any(),
                eventsLogger,
                any()
            )
        }
        confirmVerified(queryClient)
    }

    @ExperimentalCoroutinesApi
    @Test
    @Timeout(2)
    internal fun `should return a failure when scrolling fails`() = testDispatcherProvider.runTest {
        // given
        every { restClientFactory() } returns restClient
        val ctx = StepTestHelper.createStepContext<Int, Pair<Int, SearchResult<String>>>(input = 123)
        coEvery { indicesBuilder(refEq(ctx), eq(123)) } returns listOf("index-1", "index-2")
        coEvery { queryParamsBuilder(refEq(ctx), eq(123)) } returns mapOf(
            "param-1" to "value-1",
            "scroll" to "the scroll duration"
        )
        coEvery { queryBuilder(refEq(ctx), eq(123)) } returns queryNode
        every { queryNode.toString() } returns "the-query"

        val results = (1..10).mapIndexed { index, i ->
            ElasticsearchDocument("the-es-index", "_$i", index.toLong(), Instant.now(), "$i")
        }
        coEvery {
            queryClient.execute(
                any(), any(), any(), any(),
                any(),
                any(),
                any()
            )
        } returns SearchResult(
            totalResults = 10,
            results = results.subList(0, 5),
            scrollId = "the scroll ID"
        )
        coEvery {
            queryClient.scroll(
                any(), any(), any(),
                any(),
                any(),
                any()
            )
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
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1", "scroll" to "the scroll duration")),
                any(),
                eventsLogger,
                any()
            )
            queryClient.scroll(
                refEq(restClient), eq("the scroll duration"), eq("the scroll ID"),
                any(),
                eventsLogger,
                any()
            )
            queryClient.clearScroll(refEq(restClient), eq("the scroll ID"))
        }
        confirmVerified(queryClient)
    }

    @Test
    @Timeout(2)
    internal fun `should page until last document when search after is enabled`() = testDispatcherProvider.runTest {
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
            queryClient.execute(
                any(), any(), any(), any(),
                any(),
                any(),
                any()
            )
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
        val tags = emptyMap<String, String>()
        every { stepStartStopContext.toMetersTags() } returns tags
        every { stepStartStopContext.scenarioName } returns "scenario-name"
        every { stepStartStopContext.stepName } returns "step-name"
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-records", refEq(tags)) } returns recordsCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-success-bytes", refEq(tags)) } returns successCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-failure", refEq(tags)) } returns failureCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-query-success", refEq(tags)) } returns failureCounter
        every { successCounter.report(any()) } returns successCounter
        every { recordsCounter.report(any()) } returns recordsCounter
        every { receivedSuccessBytesCounter.report(any()) } returns receivedSuccessBytesCounter
        every { failureCounter.report(any()) } returns failureCounter

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
        val (input, searchResult) = (ctx.output as Channel<StepContext.StepOutputRecord<Pair<Int, SearchResult<String>>>>).receive().value

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
            queryClient.init(restClient)
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1")),
                any(),
                eventsLogger,
                any()
            )

            queryNode.set<ArrayNode>(eq("search_after"), refEq(searchBreaker1))
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-second-query"),
                eq(mapOf("param-1" to "value-1")),
                any(),
                eventsLogger,
                any()
            )

            queryNode.remove("search_after")
            queryNode.set<ArrayNode>(eq("search_after"), refEq(searchBreaker2))
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-third-query"),
                eq(mapOf("param-1" to "value-1")),
                any(),
                eventsLogger,
                any()
            )
        }
        confirmVerified(queryClient)
    }

    @ExperimentalCoroutinesApi
    @Test
    @Timeout(4)
    internal fun `should return a failure when paging next page fails`() = testDispatcherProvider.runTest {
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
            queryClient.execute(
                any(), any(), any(), any(),
                any(),
                eventsLogger,
                any()
            )
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
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-query"),
                eq(mapOf("param-1" to "value-1")),
                any(),
                eventsLogger,
                any()
            )

            queryNode.set<ArrayNode>(eq("search_after"), refEq(searchBreaker))
            queryClient.execute(
                refEq(restClient), eq(listOf("index-1", "index-2")), eq("the-second-query"),
                eq(mapOf("param-1" to "value-1")),
                any(),
                eventsLogger,
                any()
            )
        }
        confirmVerified(queryClient)
    }
}
