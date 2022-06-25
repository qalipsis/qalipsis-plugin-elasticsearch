package io.qalipsis.plugins.elasticsearch.converters

import assertk.all
import assertk.assertThat
import assertk.assertions.containsExactly
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.prop
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.mockk.coEvery
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@WithMockk
internal class JsonObjectListSingleConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @RelaxedMockK
    private lateinit var channel: Channel<ElasticsearchDocument<Int>>

    @Test
    @Timeout(5)
    @ExperimentalCoroutinesApi
    internal fun `should convert`() = testDispatcherProvider.runTest {
        // given
        val valueCounter = AtomicInteger(7)
        val record1 = relaxedMockk<ObjectNode>()
        every { record1.get("_index") } returns relaxedMockk<TextNode> { every { textValue() } returns "Index-1" }
        every { record1.get("_id") } returns relaxedMockk<TextNode> { every { textValue() } returns "ID-1" }
        val record2 = relaxedMockk<ObjectNode>()
        every { record2.get("_index") } returns relaxedMockk<TextNode> { every { textValue() } returns "Index-2" }
        every { record2.get("_id") } returns relaxedMockk<TextNode> { every { textValue() } returns "ID-2" }
        val converted = mutableListOf<ElasticsearchDocument<Int>>()
        coEvery { channel.send(capture(converted)) } answers {}

        val records: List<ObjectNode> = listOf(record1, record2)
        val capturedRecords =
            mutableListOf<ObjectNode>()
        val converter = JsonObjectListSingleConverter {
            capturedRecords.add(it)
            valueCounter.getAndIncrement()
        }

        // when
        converter.supply(
            AtomicLong(123),
            records,
            relaxedMockk { coEvery { send(any()) } coAnswers { channel.send(firstArg()) } })

        // then
        assertThat(converted).all {
            hasSize(2)
            index(0).all {
                prop(ElasticsearchDocument<Int>::index).isEqualTo("Index-1")
                prop(ElasticsearchDocument<Int>::id).isEqualTo("ID-1")
                prop(ElasticsearchDocument<Int>::receivingInstant).isNotNull()
                prop(ElasticsearchDocument<Int>::ordinal).isEqualTo(123L)
                prop(ElasticsearchDocument<Int>::value).isEqualTo(7)
            }
            index(1).all {
                prop(ElasticsearchDocument<Int>::index).isEqualTo("Index-2")
                prop(ElasticsearchDocument<Int>::id).isEqualTo("ID-2")
                prop(ElasticsearchDocument<Int>::receivingInstant).isNotNull()
                prop(ElasticsearchDocument<Int>::ordinal).isEqualTo(124L)
                prop(ElasticsearchDocument<Int>::value).isEqualTo(8)
            }
        }
        assertThat(capturedRecords).containsExactly(*records.toTypedArray())
    }

}
