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
