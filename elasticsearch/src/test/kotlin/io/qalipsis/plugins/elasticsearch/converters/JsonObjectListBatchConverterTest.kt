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
internal class JsonObjectListBatchConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @RelaxedMockK
    private lateinit var channel: Channel<List<ElasticsearchDocument<Int>>>

    @Test
    @Timeout(10)
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
