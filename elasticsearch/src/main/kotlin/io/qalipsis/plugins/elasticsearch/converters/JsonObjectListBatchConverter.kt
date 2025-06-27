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
 * Implementation of [DatasourceObjectConverter], to convert the whole result set into a unique record.
 *
 * @author Eric Jessé
 */
internal class JsonObjectListBatchConverter<T : Any>(
        private val converter: (ObjectNode) -> T
) : DatasourceObjectConverter<List<ObjectNode>, List<ElasticsearchDocument<T>>> {

    override suspend fun supply(
            offset: AtomicLong,
            value: List<ObjectNode>,
            output: StepOutput<List<ElasticsearchDocument<T>>>
    ) {
        tryAndLogOrNull(log){
            output.send(value.map { row ->
                ElasticsearchDocument(
                    row.get("_index").textValue(),
                    row.get("_id").textValue(),
                    offset.getAndIncrement(),
                    Instant.now(),
                    converter(row)
                )
            })
        }
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}
