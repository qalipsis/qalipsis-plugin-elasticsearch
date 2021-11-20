package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.json.JsonMapper
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.confirmVerified
import io.mockk.spyk
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
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

    val meterRegistry: MeterRegistry = relaxedMockk()
    val eventsLogger: EventsLogger = relaxedMockk()

    val restClient: RestClient = relaxedMockk()

    val restClientFactory: () -> RestClient = { restClient }

    val jsonMapper: JsonMapper = relaxedMockk { }

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
        reader.start(relaxedMockk())

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
        reader.start(relaxedMockk())

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
        reader.start(relaxedMockk())

        // then
        countDownLatch.await()
        verifyOnce { elasticPollStatement.reset() }
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.stop(relaxedMockk())

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
        reader.start(relaxedMockk())

        // then
        verifyOnce { elasticPollStatement.reset() }
        countDownLatch1.await()
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.stop(relaxedMockk())

        // then
        verifyOnce { elasticPollStatement.reset() }
        Assertions.assertFalse(reader.hasNext())
        delay(200)
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.start(relaxedMockk())

        // then
        countDownLatch2.await()
        verifyOnce { elasticPollStatement.reset() }
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}
