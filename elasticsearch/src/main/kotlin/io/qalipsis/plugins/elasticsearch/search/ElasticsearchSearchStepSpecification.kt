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

package io.qalipsis.plugins.elasticsearch.search

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
import org.jetbrains.annotations.NotNull


/**
 * Specification for a [io.qalipsis.plugins.elasticsearch.query.ElasticsearchDocumentsQueryStep] to search data from a Elasticsearch.
 *
 * The output is a list of [ElasticsearchDocument] contains maps of column names to values.
 *
 * @author Eric Jessé
 */
@Spec
interface ElasticsearchSearchStepSpecification<I> : Deserializable<I, Map<String, Any?>>,
    StepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>>,
    ElasticsearchStepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>>,
    ConfigurableStepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>> {

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
     * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-search-api-query-params) for the values.
     */
    fun queryParameters(paramsFactory: suspend (ctx: StepContext<*, *>, input: I) -> Map<String, String?>)

    /**
     * Builder for the Elasticsearch indices to use as target for the queries. Defaults to "_all".
     */
    fun index(indexFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<String>)

    /**
     * Builder for the JSON query to perform the search query. Defaults to all the documents in the target without sorting.
     *
     * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html).
     */
    fun query(queryFactory: suspend (ctx: StepContext<*, *>, input: I) -> String)

    /**
     * Specifies that all the documents have to be fetched, even over the result-sized window. This has to be combined
     * with either a scroll activation or a sorting definition in the query.
     *
     * The latter case will use search after to page all the data, meaning that new data coming between two requests
     * will be fetched. The requests stop when the number of fetched documents reached the total hits of the latest response.
     */
    fun fetchAll()

    /**
     * Configures the monitoring.
     */
    fun monitoring(monitoringConfiguration: StepMonitoringConfiguration.() -> Unit)
}

/**
 * Implementation of [ElasticsearchSearchStepSpecification].
 *
 * @author Eric Jessé
 */
@Spec
internal class ElasticsearchSearchStepSpecificationImpl<I> : AbstractElasticsearchQueryStepSpecification<I>(),
    ElasticsearchSearchStepSpecification<I> {

    @field:NotNull
    var indicesFactory: (suspend (ctx: StepContext<*, *>, input: I) -> List<String>) = { _, _ -> listOf("_all") }

    @field:NotNull
    var queryFactory: (suspend (ctx: StepContext<*, *>, input: I) -> String) =
        { _, _ -> """{"query":{"match_all":{}},"sort":"_id"}""" }

    internal var fetchAll = false

    override fun index(indexFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<String>) {
        this.indicesFactory = indexFactory
    }

    override fun query(queryFactory: suspend (ctx: StepContext<*, *>, input: I) -> String) {
        this.queryFactory = queryFactory
    }

    override fun fetchAll() {
        this.fetchAll = true
    }
}

/**
 * Searches data in Elasticsearch using a search query.
 *
 * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html).
 *
 * @author Eric Jessé
 */
fun <I> ElasticsearchStepSpecification<*, I, *>.search(
    configurationBlock: ElasticsearchSearchStepSpecification<I>.() -> Unit
): Deserializable<I, Map<String, Any?>> {
    val step = ElasticsearchSearchStepSpecificationImpl<I>()
    step.configurationBlock()
    this.add(step)
    return step
}
