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
    var prefix: String = "qalipsis"

    @field:NotBlank
    override var refreshInterval: String = "10s"

    override var storeSource: Boolean = false

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
