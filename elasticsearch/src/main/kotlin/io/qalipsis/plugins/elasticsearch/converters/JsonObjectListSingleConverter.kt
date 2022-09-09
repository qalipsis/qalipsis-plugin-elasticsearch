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

import com.fasterxml.jackson.databind.node.ObjectNode
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author Eric Jessé
 */
internal class JsonObjectListSingleConverter<T : Any>(
        private val converter: (ObjectNode) -> T
) : DatasourceObjectConverter<List<ObjectNode>, ElasticsearchDocument<T>> {

    override suspend fun supply(
            offset: AtomicLong,
            value: List<ObjectNode>,
            output: StepOutput<ElasticsearchDocument<T>>
    ) {
        tryAndLogOrNull(log){
            value.map { row ->
                ElasticsearchDocument(
                    row.get("_index").textValue(),
                    row.get("_id").textValue(),
                    offset.getAndIncrement(),
                    Instant.now(),
                    converter(row)
                )
            }.forEach { output.send(it) }
        }
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}
