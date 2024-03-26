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

package io.qalipsis.plugins.elasticsearch.config

import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isNotEmpty
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.plugins.elasticsearch.monitoring.meters.ElasticsearchMeasurementPublisher
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class ElasticsearchMeasurementPublisherFactoryIntegrationTest {

    @Nested
    @MicronautTest(startApplication = false, environments = ["elasticsearch"])
    inner class NoMicronautElasticMeterRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should disable the default ES measurement publisher factory`() {
            assertThat(applicationContext.getBeansOfType(CampaignMeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticsearchMeasurementPublisherFactory::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false, environments = ["elasticsearch"])
    inner class WithoutMeters : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.export.enabled" to "false",
                "meters.export.elasticsearch.enabled" to "true"
            )
        }

        @Test
        @Timeout(10)
        internal fun `should not start without ES measurement publisher factory`() {
            assertThat(applicationContext.getBeansOfType(CampaignMeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticsearchMeasurementPublisherFactory::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false, environments = ["elasticsearch"])
    inner class WithMetersButWithoutElasticsearch : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.export.enabled" to "true",
                "meters.export.elasticsearch.enabled" to "false"
            )
        }

        @Test
        @Timeout(10)
        internal fun `should not start without ES measurement publisher factory`() {
            assertThat(applicationContext.getBeansOfType(CampaignMeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticsearchMeasurementPublisher::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false, environments = ["elasticsearch"])
    inner class WithElasticsearchMeterRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.export.enabled" to "true",
                "meters.export.elasticsearch.enabled" to "true"
            )
        }

        @Test
        @Timeout(10)
        internal fun `should start with ES measurement publisher factory`() {
            assertThat(applicationContext.getBeansOfType(CampaignMeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticsearchMeasurementPublisherFactory::class.java)).isNotEmpty()
        }
    }
}