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

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNull
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.meters.MeasurementPublisherFactory
import io.qalipsis.plugins.elasticsearch.monitoring.meters.ElasticsearchMeasurementConfiguration
import io.qalipsis.plugins.elasticsearch.monitoring.meters.ElasticsearchMeasurementPublisher
import io.qalipsis.test.assertk.typedProp
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class ElasticSearchMeterRegistryConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["elasticsearch"], startApplication = false)
    inner class WithoutRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start without the measurement publisher factory`() {
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticsearchMeasurementPublisher::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(environments = ["elasticsearch"], startApplication = false)
    inner class WithConfiguredRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        internal fun `should start with the configured measurement publisher factory`() {
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).any {
                it.isInstanceOf(ElasticsearchMeasurementPublisherFactory::class)
                    .prop(ElasticsearchMeasurementPublisherFactory::getPublisher).all {
                        isInstanceOf(ElasticsearchMeasurementPublisher::class.java).typedProp<ElasticsearchMeasurementConfiguration>(
                            "configuration"
                        ).all {
                            prop(ElasticsearchMeasurementConfiguration::pathPrefix).isEqualTo("elasticsearch")
                            prop(ElasticsearchMeasurementConfiguration::urls).isEqualTo(listOf("http://localhost:9200"))
                            prop(ElasticsearchMeasurementConfiguration::username).isEqualTo("qalipsis-user")
                            prop(ElasticsearchMeasurementConfiguration::password).isEqualTo("qalipsis_password")
                            prop(ElasticsearchMeasurementConfiguration::indexPrefix).isEqualTo("qalipsis-meters")
                            prop(ElasticsearchMeasurementConfiguration::indexDatePattern).isEqualTo("yyyy-MM-dd")
                            prop(ElasticsearchMeasurementConfiguration::storeSource).isEqualTo(true)
                            prop(ElasticsearchMeasurementConfiguration::publishers).isEqualTo(2)
                            prop(ElasticsearchMeasurementConfiguration::shards).isEqualTo(1)
                            prop(ElasticsearchMeasurementConfiguration::replicas).isEqualTo(0)
                            prop(ElasticsearchMeasurementConfiguration::proxy).isEqualTo(null)
                            prop(ElasticsearchMeasurementConfiguration::refreshInterval).isEqualTo("10s")
                        }
                    }

            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.elasticsearch.enabled" to StringUtils.TRUE,
                "meters.export.enabled" to StringUtils.TRUE,
                "meters.export.elasticsearch.pathPrefix" to "elasticsearch",
                "meters.export.elasticsearch.host" to "http://localhost:9200",
                "meters.export.elasticsearch.username" to "qalipsis-user",
                "meters.export.elasticsearch.password" to "qalipsis_password",
                "meters.export.elasticsearch.indexPrefix" to "qalipsis-meters",
                "meters.export.elasticsearch.indexDatePattern" to "yyyy-MM-dd",
                "meters.export.elasticsearch.publishers" to "2",
                "meters.export.elasticsearch.storeSource" to StringUtils.TRUE,
                "meters.export.elasticsearch.prefix" to "elastic"
            )
        }
    }

    @Nested
    @MicronautTest(environments = ["elasticsearch"], startApplication = false)
    inner class WithRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        internal fun `should start with the measurement publisher factory`() {
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).any {
                it.isInstanceOf(ElasticsearchMeasurementPublisherFactory::class)
                    .prop(ElasticsearchMeasurementPublisherFactory::getPublisher).all {
                        isInstanceOf(ElasticsearchMeasurementPublisher::class.java).typedProp<ElasticsearchMeasurementConfiguration>(
                            "configuration"
                        )
                            .all {
                                prop(ElasticsearchMeasurementConfiguration::pathPrefix).isEqualTo("/")
                                prop(ElasticsearchMeasurementConfiguration::urls).isEqualTo(listOf("http://localhost:9200"))
                                prop(ElasticsearchMeasurementConfiguration::username).isNull()
                                prop(ElasticsearchMeasurementConfiguration::password).isNull()
                                prop(ElasticsearchMeasurementConfiguration::indexPrefix).isEqualTo("qalipsis-meters")
                                prop(ElasticsearchMeasurementConfiguration::indexDatePattern).isEqualTo("yyyy-MM-dd")
                                prop(ElasticsearchMeasurementConfiguration::storeSource).isEqualTo(true)
                                prop(ElasticsearchMeasurementConfiguration::publishers).isEqualTo(1)
                                prop(ElasticsearchMeasurementConfiguration::shards).isEqualTo(1)
                                prop(ElasticsearchMeasurementConfiguration::replicas).isEqualTo(0)
                                prop(ElasticsearchMeasurementConfiguration::proxy).isEqualTo(null)
                                prop(ElasticsearchMeasurementConfiguration::refreshInterval).isEqualTo("10s")
                            }
                    }
            }

            val meterRegistryFactory = applicationContext.getBean(MeasurementPublisherFactory::class.java)
            var generatedMeterRegistry = meterRegistryFactory.getPublisher()
            assertThat(generatedMeterRegistry).typedProp<ElasticsearchMeasurementConfiguration>("configuration")
                .all {
                    prop(ElasticsearchMeasurementConfiguration::pathPrefix).isEqualTo("/")
                    prop(ElasticsearchMeasurementConfiguration::urls).isEqualTo(listOf("http://localhost:9200"))
                    prop(ElasticsearchMeasurementConfiguration::username).isEqualTo(null)
                    prop(ElasticsearchMeasurementConfiguration::password).isEqualTo(null)
                    prop(ElasticsearchMeasurementConfiguration::indexPrefix).isEqualTo("qalipsis-meters")
                    prop(ElasticsearchMeasurementConfiguration::indexDatePattern).isEqualTo("yyyy-MM-dd")
                    prop(ElasticsearchMeasurementConfiguration::storeSource).isEqualTo(true)
                    prop(ElasticsearchMeasurementConfiguration::publishers).isEqualTo(1)
                    prop(ElasticsearchMeasurementConfiguration::shards).isEqualTo(1)
                    prop(ElasticsearchMeasurementConfiguration::replicas).isEqualTo(0)
                    prop(ElasticsearchMeasurementConfiguration::proxy).isEqualTo(null)
                    prop(ElasticsearchMeasurementConfiguration::refreshInterval).isEqualTo("10s")
                }


            generatedMeterRegistry = meterRegistryFactory.getPublisher()
            assertThat(generatedMeterRegistry).typedProp<ElasticsearchMeasurementConfiguration>("configuration")
                .all {
                    prop(ElasticsearchMeasurementConfiguration::pathPrefix).isEqualTo("/")
                    prop(ElasticsearchMeasurementConfiguration::urls).isEqualTo(listOf("http://localhost:9200"))
                    prop(ElasticsearchMeasurementConfiguration::username).isEqualTo(null)
                    prop(ElasticsearchMeasurementConfiguration::password).isEqualTo(null)
                    prop(ElasticsearchMeasurementConfiguration::indexPrefix).isEqualTo("qalipsis-meters")
                    prop(ElasticsearchMeasurementConfiguration::indexDatePattern).isEqualTo("yyyy-MM-dd")
                    prop(ElasticsearchMeasurementConfiguration::storeSource).isEqualTo(true)
                    prop(ElasticsearchMeasurementConfiguration::publishers).isEqualTo(1)
                    prop(ElasticsearchMeasurementConfiguration::shards).isEqualTo(1)
                    prop(ElasticsearchMeasurementConfiguration::replicas).isEqualTo(0)
                    prop(ElasticsearchMeasurementConfiguration::proxy).isEqualTo(null)
                    prop(ElasticsearchMeasurementConfiguration::refreshInterval).isEqualTo("10s")
                }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.elasticsearch.enabled" to StringUtils.TRUE,
                "meters.export.enabled" to StringUtils.TRUE,
                "meters.export.elasticsearch.index" to "qalipsis-meters"
            )
        }
    }
}