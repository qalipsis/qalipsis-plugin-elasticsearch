/*
 * Copyright 2024 AERIS IT Solutions GmbH
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

package io.qalipsis.plugins.elasticsearch.monitoring;

import io.micronaut.core.annotation.Nullable
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

/**
 * Configuration for Elasticsearch Publishing.
 *
 * @property urls list of URLs to the Elasticsearch instances defaults to http://localhost:9200.
 * @property refreshInterval delay to refresh the indices in ES, defaults to 10s.
 * @property storeSource stores the source of the documents and not just the values, defaults to false.
 * @property indexDatePattern format of the date part of the index as supported by [java.time.format.DateTimeFormatter] defaults to uuuu-MM-dd to create an index per day.
 * @property publishers number of concurrent publication of events that can be run defaults to 1 (no concurrency).
 * @property username name of the user to use for basic authentication when connecting to Elasticsearch.
 * @property password password of the user to use for basic authentication when connecting to Elasticsearch.
 * @property shards number of shards to apply on the created indices for events, defaults to 1.
 * @property replicas number of replicas to apply on the created indices for events, defaults to 0.
 * @property proxy URL of the http proxy to use to access to Elasticsearch it might be convenient in order to support other kind of authentication in Elasticsearch.
 *
 * @author Eric Jessé
 */
internal interface MonitoringConfiguration {

    @get:NotEmpty
    var urls: List<@NotBlank String>

    @get:NotBlank
    var pathPrefix: String

    @get:NotBlank
    var indexPrefix: String

    @get:NotBlank
    var refreshInterval: String

    @get:NotNull
    var storeSource: Boolean

    @get:NotBlank
    var indexDatePattern: String

    @get:Min(1)
    var publishers: Int

    @get:Nullable
    var username: String?

    @get:Nullable
    var password: String?

    @get:Min(1)
    var shards: Int

    @get:Min(0)
    var replicas: Int

    @get:Nullable
    var proxy: String?

}
