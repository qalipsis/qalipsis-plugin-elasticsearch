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

import com.fasterxml.jackson.databind.json.JsonMapper
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.coVerifyNever
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyOnce
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration

/**
 *
 * @author Eric Jessé
 */
@WithMockk
internal class ElasticsearchIterativeReaderTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    val elasticPollStatement: ElasticsearchPollStatement = relaxedMockk()

    val meterRegistry: CampaignMeterRegistry = relaxedMockk()

    val eventsLogger: EventsLogger = relaxedMockk()

    val restClient: RestClient = relaxedMockk()

    val restClientFactory: () -> RestClient = { restClient }

    val jsonMapper: JsonMapper = relaxedMockk { }

    @RelaxedMockK
    private lateinit var stepStartStopContext: StepStartStopContext

    private val recordsByteCounter = relaxedMockk<Counter>()

    private val receivedSuccessBytesCounter = relaxedMockk<Counter>()

    private val successCounter = relaxedMockk<Counter>()

    private val failureCounter = relaxedMockk<Counter>()

    @Test
    @Timeout(10)
    internal fun `should have no next when not running`() = testDispatcherProvider.run {
        // given
        val reader = spyk(
            ElasticsearchIterativeReader(
                this,
                ioCoroutineContext = testDispatcherProvider.io(),
                restClientFactory,
                elasticPollStatement,
                "Any",
                emptyMap(),
                Duration.ofMillis(100),
                jsonMapper,
                { Channel(1) },
                meterRegistry,
                eventsLogger
            )
        )
        coEvery { reader.coInvokeInvisible<Unit>("poll", any<RestClient>()) } returns Unit

        // when + then
        Assertions.assertFalse(reader.hasNext())
        delay(200)
        coVerifyNever { reader.coInvokeInvisible<Unit>("poll", any<RestClient>()) }
    }

    @Test
    @Timeout(20)
    internal fun `should have next when running and poll`() = testDispatcherProvider.run {
        // given
        val countDownLatch = SuspendedCountLatch(3, true)

        val tags = emptyMap<String, String>()
        every { stepStartStopContext.toMetersTags() } returns tags
        every { stepStartStopContext.scenarioName } returns "scenario-name"
        every { stepStartStopContext.stepName } returns "step-name"
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-byte-records", refEq(tags)) } returns recordsByteCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-success-bytes", refEq(tags)) } returns successCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-failure", refEq(tags)) } returns failureCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-success", refEq(tags)) } returns failureCounter
        every { successCounter.report(any()) } returns successCounter
        every { recordsByteCounter.report(any()) } returns recordsByteCounter
        every { receivedSuccessBytesCounter.report(any()) } returns receivedSuccessBytesCounter
        every { failureCounter.report(any()) } returns failureCounter

        val reader = spyk(
            ElasticsearchIterativeReader(
                this,
                ioCoroutineContext = testDispatcherProvider.io(),
                restClientFactory,
                elasticPollStatement,
                "Any",
                emptyMap(),
                Duration.ofMillis(100),
                jsonMapper,
                { Channel(1) },
                meterRegistry,
                eventsLogger
            )
        )
        coEvery { reader.coInvokeInvisible<Unit>("poll", any<RestClient>()) } coAnswers { countDownLatch.decrement() }

        // when
        reader.start(stepStartStopContext)

        // then
        Assertions.assertTrue(reader.hasNext())
        countDownLatch.await()
        verifyOnce { elasticPollStatement.reset() }
        confirmVerified(elasticPollStatement)
    }

    @Test
    @Timeout(20)
    internal fun `should keep on polling even after a failure`() = testDispatcherProvider.run {
        // given
        val countDownLatch = SuspendedCountLatch(3, true)
        val tags = emptyMap<String, String>()
        every { stepStartStopContext.toMetersTags() } returns tags
        every { stepStartStopContext.scenarioName } returns "scenario-name"
        every { stepStartStopContext.stepName } returns "step-name"
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-byte-records", refEq(tags)) } returns recordsByteCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-success-bytes", refEq(tags)) } returns successCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-failure", refEq(tags)) } returns failureCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-success", refEq(tags)) } returns failureCounter
        every { successCounter.report(any()) } returns successCounter
        every { recordsByteCounter.report(any()) } returns recordsByteCounter
        every { receivedSuccessBytesCounter.report(any()) } returns receivedSuccessBytesCounter
        every { failureCounter.report(any()) } returns failureCounter
        val reader = spyk(
            ElasticsearchIterativeReader(
                this,
                ioCoroutineContext = testDispatcherProvider.io(),
                restClientFactory,
                elasticPollStatement,
                "Any",
                emptyMap(),
                Duration.ofMillis(100),
                jsonMapper,
                { Channel(1) },
                meterRegistry,
                eventsLogger
            )
        )
        coEvery { reader.coInvokeInvisible<Unit>("poll", any<RestClient>()) } coAnswers {
            countDownLatch.decrement()
            throw RuntimeException("")
        }

        // when
        reader.start(stepStartStopContext)

        // then
        countDownLatch.await()
        Assertions.assertTrue(reader.hasNext())
        verifyOnce { elasticPollStatement.reset() }
        confirmVerified(elasticPollStatement)
    }

    @Test
    @Timeout(20)
    internal fun `should be stoppable`() = testDispatcherProvider.run {
        // given
        val countDownLatch = SuspendedCountLatch(3, true)
        val tags = emptyMap<String, String>()
        every { stepStartStopContext.toMetersTags() } returns tags
        every { stepStartStopContext.scenarioName } returns "scenario-name"
        every { stepStartStopContext.stepName } returns "step-name"
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-byte-records", refEq(tags)) } returns recordsByteCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-success-bytes", refEq(tags)) } returns successCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-failure", refEq(tags)) } returns failureCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-success", refEq(tags)) } returns failureCounter
        every { successCounter.report(any()) } returns successCounter
        every { recordsByteCounter.report(any()) } returns recordsByteCounter
        every { receivedSuccessBytesCounter.report(any()) } returns receivedSuccessBytesCounter
        every { failureCounter.report(any()) } returns failureCounter
        val reader = spyk(
            ElasticsearchIterativeReader(
                this,
                ioCoroutineContext = testDispatcherProvider.io(),
                restClientFactory,
                elasticPollStatement,
                "Any",
                emptyMap(),
                Duration.ofMillis(100),
                jsonMapper,
                { Channel(1) },
                meterRegistry,
                eventsLogger
            )
        )
        coEvery { reader.coInvokeInvisible<Unit>("poll", any<RestClient>()) } coAnswers { countDownLatch.decrement() }

        // when
        reader.start(stepStartStopContext)

        // then
        countDownLatch.await()
        verifyOnce { elasticPollStatement.reset() }
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.stop(stepStartStopContext)

        // then
        verifyOnce { elasticPollStatement.reset() }
        Assertions.assertFalse(reader.hasNext())
        Thread.sleep(200)
        coVerifyNever { reader.coInvokeInvisible<Unit>("poll", any<RestClient>()) }
    }

    @Test
    @Timeout(20)
    internal fun `should be restartable`() = testDispatcherProvider.run {
        // given
        // Count down for the first period of activity.
        val countDownLatch1 = SuspendedCountLatch(3)
        // Count down for the second period of activity.
        val countDownLatch2 = SuspendedCountLatch(3, true)
        val tags = emptyMap<String, String>()
        every { stepStartStopContext.toMetersTags() } returns tags
        every { stepStartStopContext.scenarioName } returns "scenario-name"
        every { stepStartStopContext.stepName } returns "step-name"
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-byte-records", refEq(tags)) } returns recordsByteCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-success-bytes", refEq(tags)) } returns successCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-failure", refEq(tags)) } returns failureCounter
        every { meterRegistry.counter("scenario-name", "step-name", "elasticsearch-poll-success", refEq(tags)) } returns failureCounter
        every { successCounter.report(any()) } returns successCounter
        every { recordsByteCounter.report(any()) } returns recordsByteCounter
        every { receivedSuccessBytesCounter.report(any()) } returns receivedSuccessBytesCounter
        every { failureCounter.report(any()) } returns failureCounter
        val reader = spyk(
            ElasticsearchIterativeReader(
                this,
                ioCoroutineContext = testDispatcherProvider.io(),
                restClientFactory,
                elasticPollStatement,
                "Any",
                emptyMap(),
                Duration.ofMillis(100),
                jsonMapper,
                { Channel(Channel.UNLIMITED) },
                meterRegistry,
                eventsLogger
            )
        )
        coEvery { reader.coInvokeInvisible<Unit>("poll", any<RestClient>()) } coAnswers {
            if (countDownLatch1.get() > 0) {
                countDownLatch1.decrement()
            } else {
                countDownLatch2.decrement()
            }
        }

        // when
        reader.start(stepStartStopContext)

        // then
        verifyOnce { elasticPollStatement.reset() }
        countDownLatch1.await()
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.stop(stepStartStopContext)

        // then
        verifyOnce { elasticPollStatement.reset() }
        Assertions.assertFalse(reader.hasNext())
        delay(200)
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.start(stepStartStopContext)

        // then
        countDownLatch2.await()
        verifyOnce { elasticPollStatement.reset() }
    }
}
