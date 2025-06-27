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

package io.qalipsis.plugins.elasticsearch.save

import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.plugins.elasticsearch.Document

/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform inserts into Elasticsearch.
 *
 * @property elasticsearchSaveQueryClient client to save documents in Elasticsearch.
 * @property documentsFactory closure to generate a list of [Document].
 *
 * @author Alex Averyanov
 */
internal class ElasticsearchSaveStep<I>(
    id: StepName,
    retryPolicy: RetryPolicy?,
    private val elasticsearchSaveQueryClient: ElasticsearchSaveQueryClient,
    private val documentsFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<Document>,
) : AbstractStep<I, ElasticsearchSaveResult<I>>(id, retryPolicy) {

    override suspend fun start(context: StepStartStopContext) {
        init()
        elasticsearchSaveQueryClient.start(context)
    }

    override suspend fun execute(context: StepContext<I, ElasticsearchSaveResult<I>>) {
        val input = context.receive()
        val documents = documentsFactory(context, input)
        val (responseBody, meters) = elasticsearchSaveQueryClient.execute(documents, context.toEventTags())

        context.send(ElasticsearchSaveResult(input, documents, responseBody, meters))
    }

    override suspend fun stop(context: StepStartStopContext) {
        elasticsearchSaveQueryClient.stop(context)
    }
}
