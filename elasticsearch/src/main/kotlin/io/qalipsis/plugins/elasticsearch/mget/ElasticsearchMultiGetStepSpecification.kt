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

package io.qalipsis.plugins.elasticsearch.mget

import com.fasterxml.jackson.databind.json.JsonMapper
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.elasticsearch.AbstractElasticsearchQueryStepSpecification
import io.qalipsis.plugins.elasticsearch.Deserializable
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.plugins.elasticsearch.ElasticsearchStepSpecification
import io.qalipsis.plugins.elasticsearch.query.SearchResult
import org.elasticsearch.client.RestClient


/**
 * Specification for a [io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryStep] to
 * fetch documents using a multi-get request.
 *
 * The output is a list of [ElasticsearchDocument] contains maps of column names to values.
 *
 * @author Eric Jessé
 */
@Spec
interface ElasticsearchMultiGetStepSpecification<I> : Deserializable<I, Map<String, Any?>>,
    StepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>>,
    ConfigurableStepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>>,
    ElasticsearchStepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>> {

    /**
     * Configures the REST client to connect to Elasticsearch.
     */
    fun client(client: () -> RestClient)

    /**
     * Configures the JSON Mapper to deserialize the records.
     */
    fun mapper(mapper: (JsonMapper) -> Unit)

    /**
     * Builder for the options to add as query parameters. Defaults to no parameter.
     *
     * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html#docs-multi-get-api-query-params) for the values.
     */
    fun queryParameters(paramsFactory: suspend (ctx: StepContext<*, *>, input: I) -> Map<String, String?>)

    /**
     * Builder for the JSON query to perform the multi get query.
     *
     * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html).
     */
    fun query(queryFactory: suspend MultiGetQueryBuilder.(ctx: StepContext<*, *>, input: I) -> Unit)

    /**
     * Configures the monitoring of the step.
     */
    fun monitoring(monitoringConfiguration: StepMonitoringConfiguration.() -> Unit)
}

/**
 * Implementation of [ElasticsearchMultiGetStepSpecification].
 *
 * @author Eric Jessé
 */
@Spec
internal class ElasticsearchMultiGetStepSpecificationImpl<I> : AbstractElasticsearchQueryStepSpecification<I>(),
    ElasticsearchMultiGetStepSpecification<I> {

    internal var queryFactory: suspend MultiGetQueryBuilder.(ctx: StepContext<*, *>, input: I) -> Unit = { _, _ -> }

    override fun query(queryFactory: suspend MultiGetQueryBuilder.(ctx: StepContext<*, *>, input: I) -> Unit) {
        this.queryFactory = queryFactory
    }

}

/**
 * Searches data in Elasticsearch using a multi-get query.
 *
 * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html).
 *
 * @author Eric Jessé
 */
fun <I> ElasticsearchStepSpecification<*, I, *>.mget(
    configurationBlock: ElasticsearchMultiGetStepSpecification<I>.() -> Unit
): Deserializable<I, Map<String, Any?>> {
    val step = ElasticsearchMultiGetStepSpecificationImpl<I>()
    step.configurationBlock()
    this.add(step)
    return step
}
