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
