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

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.qalipsis.api.meters.MeasurementPublisher
import io.qalipsis.api.meters.MeasurementPublisherFactory
import io.qalipsis.plugins.elasticsearch.config.ElasticsearchMeasurementPublisherFactory
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils.containsOnly

/**
 * Tests to verify the automatic configuration of the Elasticsearch publisher.
 */
class ElasticsearchMeasurementPublisherConfigurationIntegrationTest {

    @Nested
    @MicronautTest(propertySources = ["classpath:application-elasticsearch.yml"])
    inner class NoPublisher {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should start without publisher`() {
            assertThat(applicationContext.getBeansOfType(MeasurementPublisher::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticsearchMeasurementConfiguration::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(environments = ["withpublisher"])
    inner class WithPublisher {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should start with the publisher`() {
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).all {
                hasSize(1)
                any {
                    it.isInstanceOf(ElasticsearchMeasurementPublisherFactory::class)
                        .prop(ElasticsearchMeasurementPublisherFactory::getPublisher).isNotNull()
                }
            }
            val configuration = applicationContext.getBean(ElasticsearchMeasurementConfiguration::class.java)
            assertThat(configuration).all {
                prop(ElasticsearchMeasurementConfiguration::urls).all {
                    hasSize(2)
                    containsOnly("http://localhost:9205", "http://localhost:9203")
                }
                prop(ElasticsearchMeasurementConfiguration::pathPrefix).isEqualTo("/es/")
                prop(ElasticsearchMeasurementConfiguration::indexPrefix).isEqualTo("my-qali-meters")
                prop(ElasticsearchMeasurementConfiguration::refreshInterval).isEqualTo("1m")
                prop(ElasticsearchMeasurementConfiguration::indexDatePattern).isEqualTo("yyyy-MM")
                prop(ElasticsearchMeasurementConfiguration::publishers).isEqualTo(1)
                prop(ElasticsearchMeasurementConfiguration::username).isEqualTo("the-user")
                prop(ElasticsearchMeasurementConfiguration::password).isEqualTo("the-password")
                prop(ElasticsearchMeasurementConfiguration::shards).isEqualTo(7)
                prop(ElasticsearchMeasurementConfiguration::replicas).isEqualTo(2)
                prop(ElasticsearchMeasurementConfiguration::proxy).isEqualTo("http://localhost:4000")
            }

        }
    }

}
