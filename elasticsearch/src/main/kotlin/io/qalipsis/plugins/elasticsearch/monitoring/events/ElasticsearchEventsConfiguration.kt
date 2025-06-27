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

    override var storeSource: Boolean = true

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
