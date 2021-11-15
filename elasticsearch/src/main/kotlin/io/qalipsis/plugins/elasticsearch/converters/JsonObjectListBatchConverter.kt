package io.qalipsis.plugins.elasticsearch.converters

import com.fasterxml.jackson.databind.node.ObjectNode
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import kotlinx.coroutines.channels.SendChannel
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
