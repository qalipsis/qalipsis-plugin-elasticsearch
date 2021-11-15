package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.json.JsonMapper
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.spyk
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.elasticsearch.query.ElasticsearchQueryMetrics
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.coVerifyExactly
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
                ElasticsearchQueryMetrics(),
                jsonMapper,
                { Channel(1) }
            ), recordPrivateCalls = true
        )
        coEvery { reader["poll"](any<RestClient>()) } returns Unit


        // when + then
        Assertions.assertFalse(reader.hasNext())
        delay(200)
        coVerifyNever { reader["poll"](any<RestClient>()) }
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
                ElasticsearchQueryMetrics(),
                jsonMapper,
                { Channel(1) }
            ), recordPrivateCalls = true
        )
        coEvery { reader["poll"](any<RestClient>()) } coAnswers { countDownLatch.decrement() }


        // when
        reader.start(relaxedMockk())

        // then
        Assertions.assertTrue(reader.hasNext())
        countDownLatch.await()
        verifyOnce { elasticPollStatement.reset() }
        coVerify(atLeast = 3) {
            reader["poll"](refEq(restClient))
        }
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
                ElasticsearchQueryMetrics(),
                jsonMapper,
                { Channel(1) }
            ), recordPrivateCalls = true
        )
        coEvery { reader["poll"](any<RestClient>()) } coAnswers {
            countDownLatch.decrement()
            throw RuntimeException("")
        }


        // when
        reader.start(relaxedMockk())

        // then
        countDownLatch.await()
        Assertions.assertTrue(reader.hasNext())
        verifyOnce { elasticPollStatement.reset() }
        coVerify(atLeast = 3) {
            reader["poll"](refEq(restClient))
        }
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
                ElasticsearchQueryMetrics(),
                jsonMapper,
                { Channel(1) }
            ), recordPrivateCalls = true
        )
        coEvery { reader["poll"](any<RestClient>()) } coAnswers { countDownLatch.decrement() }


        // when
        reader.start(relaxedMockk())

        // then
        countDownLatch.await()
        verifyOnce { elasticPollStatement.reset() }
        coVerify(atLeast = 3) {
            reader["poll"](refEq(restClient))
        }
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.stop(relaxedMockk())

        // then
        verifyOnce { elasticPollStatement.reset() }
        Assertions.assertFalse(reader.hasNext())
        Thread.sleep(200)
        coVerifyNever { reader["poll"](any<RestClient>()) }
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
                ElasticsearchQueryMetrics(),
                jsonMapper,
                { Channel(Channel.UNLIMITED) }
            ), recordPrivateCalls = true
        )
        coEvery { reader["poll"](any<RestClient>()) } coAnswers {
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
        coVerifyExactly(3) {
            reader["poll"](refEq(restClient))
        }
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.stop(relaxedMockk())

        // then
        verifyOnce { elasticPollStatement.reset() }
        Assertions.assertFalse(reader.hasNext())
        delay(200)
        coVerifyNever { reader["poll"](any<RestClient>()) }
        clearMocks(reader, elasticPollStatement, answers = false)

        // when
        reader.start(relaxedMockk())

        // then
        countDownLatch2.await()
        verifyOnce { elasticPollStatement.reset() }
        coVerify(atLeast = 3) {
            reader["poll"](refEq(restClient))
        }
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}
