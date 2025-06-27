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

package io.qalipsis.plugins.elasticsearch.config

import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.Executors
import io.qalipsis.api.config.MetersConfig
import io.qalipsis.api.meters.MeasurementPublisherFactory
import io.qalipsis.plugins.elasticsearch.monitoring.meters.ElasticsearchMeasurementConfiguration
import io.qalipsis.plugins.elasticsearch.monitoring.meters.ElasticsearchMeasurementConfiguration.Companion.ELASTICSEARCH_ENABLED
import io.qalipsis.plugins.elasticsearch.monitoring.meters.ElasticsearchMeasurementPublisher
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope

/**
 * Configuration to get the measurement publisher for the export of qalipsis meters to Elasticsearch.
 *
 * @author Eric Jessé
 */
@Singleton
@Requirements(
    Requires(property = MetersConfig.EXPORT_ENABLED, value = StringUtils.TRUE),
    Requires(property = ELASTICSEARCH_ENABLED, value = StringUtils.TRUE)
)
internal class ElasticsearchMeasurementPublisherFactory(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineScope: CoroutineScope,
    private val configuration: ElasticsearchMeasurementConfiguration
) : MeasurementPublisherFactory {

    override fun getPublisher(): ElasticsearchMeasurementPublisher {
        return ElasticsearchMeasurementPublisher(
            coroutineScope = coroutineScope,
            configuration = configuration
        )
    }

}
