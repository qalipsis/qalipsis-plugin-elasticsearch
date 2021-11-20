package io.qalipsis.plugins.elasticsearch.converters

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.mockk.coEvery
import io.mockk.coJustRun
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.slot
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runBlockingTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@WithMockk
internal class JsonObjectListBatchConverterTest {

    @RelaxedMockK
    private lateinit var channel: Channel<List<ElasticsearchDocument<Int>>>

    @Test
    @Timeout(10)
    @ExperimentalCoroutinesApi
    internal fun `should convert`() = runBlockingTest {
        // given
        val valueCounter = AtomicInteger(7)
        val record1 = relaxedMockk<ObjectNode>()
        every { record1.get("_index") } returns relaxedMockk<TextNode> { every { textValue() } returns "Index-1" }
        every { record1.get("_id") } returns relaxedMockk<TextNode> { every { textValue() } returns "ID-1" }
        val record2 = relaxedMockk<ObjectNode>()
        every { record2.get("_index") } returns relaxedMockk<TextNode> { every { textValue() } returns "Index-2" }
        every { record2.get("_id") } returns relaxedMockk<TextNode> { every { textValue() } returns "ID-2" }
        val converted = slot<List<ElasticsearchDocument<Int>>>()
        coJustRun { channel.send(capture(converted)) }

        val records: List<ObjectNode> = listOf(record1, record2)
        val capturedRecords = mutableListOf<ObjectNode>()
        val converter = JsonObjectListBatchConverter {
            capturedRecords.add(it)
            valueCounter.getAndIncrement()
        }

        // when
        converter.supply(
            AtomicLong(123),
            records,
            relaxedMockk { coEvery { send(any()) } coAnswers { channel.send(firstArg()) } })

        // then
        assertThat(converted.captured).all {
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