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

package io.qalipsis.plugins.elasticsearch.monitoring.meters

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Introspected
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.config.MetersConfig
import io.qalipsis.plugins.elasticsearch.monitoring.MonitoringConfiguration
import io.qalipsis.plugins.elasticsearch.monitoring.meters.ElasticsearchMeasurementConfiguration.Companion.ELASTICSEARCH_CONFIGURATION
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty

/**
 * Custom [MonitoringConfiguration] adapted for publishing of meters to elasticsearch.
 *
 * @property indexPrefix prefix to use for the created indices containing the events defaults to qalipsis-events.
 * @property prefix property prefix to prepend to configuration names, defaults to elastic.
 *
 * @author Francisca Eze
 */
@Introspected
@Requires(property = "$ELASTICSEARCH_CONFIGURATION.enabled", value = StringUtils.TRUE)
@ConfigurationProperties(ELASTICSEARCH_CONFIGURATION)
internal class ElasticsearchMeasurementConfiguration: MonitoringConfiguration {

    @field:NotEmpty
    override var urls: List<@NotBlank String> = listOf("http://localhost:9200")

    @field:NotBlank
    override var pathPrefix: String = "/"

    @field:NotBlank
    override var indexPrefix: String = "qalipsis-meters"

    @field:NotBlank
    override var refreshInterval: String = "10s"

    override var storeSource: Boolean = true

    @field:NotBlank
    override var indexDatePattern: String = "yyyy-MM-dd"

    @field:Min(1)
    override var publishers: Int = 1

    override var username: String? = null

    override var password: String? = null

    @field:Min(1)
    override var shards: Int = 1

    @field:Min(0)
    override var replicas: Int = 0

    override var proxy: String? = null

    companion object {

        const val ELASTICSEARCH_CONFIGURATION = "${MetersConfig.EXPORT_CONFIGURATION}.elasticsearch"

        const val ELASTICSEARCH_ENABLED = "${ELASTICSEARCH_CONFIGURATION}.enabled"
    }
}
