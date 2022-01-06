package io.qalipsis.plugins.elasticsearch.save

import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.plugins.elasticsearch.Document

/**
 * Client to save documents to Elasticsearch using the bulk API.
 *
 * @author Alex Averyanov
 */
internal interface ElasticsearchSaveQueryClient {

    /**
     * Initializes the client and connects to the Elasticsearch server.
     */
    suspend fun start(context: StepStartStopContext)

    /**
     * Indexes documents into the Elasticsearch server.
     */
    suspend fun execute(
        records: List<Document>,
        contextEventTags: Map<String, String>
    ): ElasticsearchBulkResult

    /**
     * Cleans the client and closes the connections to the Elasticsearch server.
     */
    suspend fun stop(context: StepStartStopContext)
}
