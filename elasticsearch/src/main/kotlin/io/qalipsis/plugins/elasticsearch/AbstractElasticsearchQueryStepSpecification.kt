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
