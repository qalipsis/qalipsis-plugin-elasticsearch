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

package io.qalipsis.plugins.elasticsearch.monitoring.events

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Introspected
import io.qalipsis.api.config.EventsConfig
import io.qalipsis.api.constraints.PositiveDuration
import io.qalipsis.api.events.EventLevel
import io.qalipsis.plugins.elasticsearch.monitoring.MonitoringConfiguration
import java.time.Duration
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

/**
 * Custom [MonitoringConfiguration] adapted for publishing of events to elasticsearch.
 *
 * @property minLevel minimal accepted level of events defaults to INFO.
 * @property indexPrefix prefix to use for the created indices containing the events defaults to qalipsis-events.
 * @property lingerPeriod maximal period between two publication of events to Elasticsearch defaults to 10 seconds.
 * @property batchSize maximal number of events buffered between two publications of events to Elasticsearch defaults to 2000.
 *
 * @author Eric Jessé
 */
@Introspected
@Requires(property = "${EventsConfig.EXPORT_CONFIGURATION}.elasticsearch.enabled", value = "true")
@ConfigurationProperties("${EventsConfig.EXPORT_CONFIGURATION}.elasticsearch")
internal class ElasticsearchEventsConfiguration : MonitoringConfiguration {

    @field:NotNull
    var minLevel: EventLevel = EventLevel.INFO

    @field:NotEmpty
    override var urls: List<@NotBlank String> = listOf("http://localhost:9200")

    @field:NotBlank
    override var pathPrefix: String = "/"

    @field:NotBlank
    override var indexPrefix: String = "qalipsis-events"

    @field:NotBlank
    override var refreshInterval: String = "10s"

    override var storeSource: Boolean = false

    @field:NotBlank
    override var indexDatePattern: String = "yyyy-MM-dd"

    @field:PositiveDuration
    var lingerPeriod: Duration = Duration.ofSeconds(10)

    @field:Min(1)
    var batchSize: Int = 40000

    @field:Min(1)
    override var publishers: Int = 1

    override var username: String? = null

    override var password: String? = null

    @field:Min(1)
    override var shards: Int = 1

    @field:Min(0)
    override var replicas: Int = 0

    override var proxy: String? = null
}
