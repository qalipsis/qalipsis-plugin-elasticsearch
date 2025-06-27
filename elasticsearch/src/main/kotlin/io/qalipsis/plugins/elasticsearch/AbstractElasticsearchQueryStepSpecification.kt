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

package io.qalipsis.plugins.elasticsearch

import com.fasterxml.jackson.databind.json.JsonMapper
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.elasticsearch.query.SearchResult
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import kotlin.reflect.KClass

/**
 * Parent
 *
 * @author Eric Jessé
 */
internal abstract class AbstractElasticsearchQueryStepSpecification<I> :
    AbstractStepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>>(),
    Deserializable<I, Map<String, Any?>>,
    StepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>>,
    ElasticsearchStepSpecification<I, Pair<I, SearchResult<Map<String, Any?>>>, Deserializable<I, Map<String, Any?>>> {

    internal open var client: (() -> RestClient) = { RestClient.builder(HttpHost("localhost", 9200, "http")).build() }

    internal open var mapper: ((JsonMapper) -> Unit) = { }

    internal open var paramsFactory: (suspend (ctx: StepContext<*, *>, input: I) -> Map<String, String?>) =
        { _, _ -> emptyMap() }

    internal var convertFullDocument = false

    internal var targetClass: KClass<*> = Map::class

    internal var monitoringConfig = StepMonitoringConfiguration()

    open fun client(client: () -> RestClient) {
        this.client = client
    }

    open fun mapper(mapper: (JsonMapper) -> Unit) {
        this.mapper = mapper
    }

    open fun queryParameters(paramsFactory: suspend (ctx: StepContext<*, *>, input: I) -> Map<String, String?>) {
        this.paramsFactory = paramsFactory
    }

    open fun monitoring(monitoringConfiguration: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfiguration()
    }

    override fun <O : Any> deserialize(
        targetClass: KClass<O>,
        fullDocument: Boolean
    ): StepSpecification<I, Pair<I, SearchResult<O>>, *> {
        this.targetClass = targetClass
        this.convertFullDocument = fullDocument

        @Suppress("UNCHECKED_CAST")
        return this as StepSpecification<I, Pair<I, SearchResult<O>>, *>
    }

}
