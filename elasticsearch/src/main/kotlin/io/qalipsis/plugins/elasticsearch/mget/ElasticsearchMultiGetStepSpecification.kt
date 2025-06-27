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
