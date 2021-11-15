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
