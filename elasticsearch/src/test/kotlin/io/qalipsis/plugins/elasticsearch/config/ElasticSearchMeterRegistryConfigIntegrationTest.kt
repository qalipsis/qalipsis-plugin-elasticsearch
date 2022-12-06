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

package io.qalipsis.plugins.elasticsearch.config

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNull
import assertk.assertions.prop
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.elastic.ElasticConfig
import io.micrometer.elastic.ElasticMeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.meters.MeterRegistryConfiguration
import io.qalipsis.api.meters.MeterRegistryFactory
import io.qalipsis.test.assertk.typedProp
import jakarta.inject.Inject
import java.time.Duration
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
        fun `should start without the registry`() {
            assertThat(applicationContext.getBeansOfType(ElasticMeterRegistry::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(MeterRegistryFactory::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(environments = ["elasticsearch"], startApplication = false)
    inner class WithConfiguredRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        internal fun `should start with the configured registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).any {
                it.isInstanceOf(ElasticMeterRegistry::class)
            }

            assertThat(applicationContext.getBean(ElasticMeterRegistry::class.java)).typedProp<ElasticConfig>("config").all {
                prop(ElasticConfig::prefix).isEqualTo("elasticsearch")
                prop(ElasticConfig::host).isEqualTo("http://localhost:9200")
                prop(ElasticConfig::userName).isEqualTo("qalipsis-user")
                prop(ElasticConfig::password).isEqualTo("qalipsis_password")
                prop(ElasticConfig::index).isEqualTo("qalipsis-meters")
                prop(ElasticConfig::indexDateFormat).isEqualTo("yyyy-MM-dd")
                prop(ElasticConfig::autoCreateIndex).isEqualTo(true)
                prop(ElasticConfig::timestampFieldName).isEqualTo("@timestamp")
                prop(ElasticConfig::step).isEqualTo(Duration.ofMinutes(6))
            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.elasticsearch.enabled" to StringUtils.TRUE,
                "meters.export.elasticsearch.host" to "http://localhost:9200",
                "meters.export.elasticsearch.user-name" to "qalipsis-user",
                "meters.export.elasticsearch.password" to "qalipsis_password",
                "meters.export.elasticsearch.index" to "qalipsis-meters",
                "meters.export.elasticsearch.step" to "PT6M",
                "meters.export.elasticsearch.index-date-format" to "yyyy-MM-dd",
                "meters.export.elasticsearch.auto-create-index" to StringUtils.TRUE
            )
        }
    }

    @Nested
    @MicronautTest(environments = ["elasticsearch"], startApplication = false)
    inner class WithRegistry : TestPropertyProvider{

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        internal fun `should start with the registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).any {
                it.isInstanceOf(ElasticMeterRegistry::class)
            }

            assertThat(applicationContext.getBean(ElasticMeterRegistry::class.java)).typedProp<ElasticConfig>("config").all {
                prop(ElasticConfig::prefix).isEqualTo("elasticsearch")
                prop(ElasticConfig::host).isEqualTo("http://localhost:9200")
                prop(ElasticConfig::userName).isNull()
                prop(ElasticConfig::password).isNull()
                prop(ElasticConfig::index).isEqualTo("qalipsis-meters")
                prop(ElasticConfig::indexDateFormat).isEqualTo("yyyy-MM-dd")
                prop(ElasticConfig::autoCreateIndex).isEqualTo(true)
                prop(ElasticConfig::timestampFieldName).isEqualTo("@timestamp")
                prop(ElasticConfig::step).isEqualTo(Duration.ofSeconds(10))
            }

            val meterRegistryFactory = applicationContext.getBean(MeterRegistryFactory::class.java)
            var generatedMeterRegistry = meterRegistryFactory.getRegistry(
                object : MeterRegistryConfiguration {
                    override val step: Duration? = null
                }
            )
            assertThat(generatedMeterRegistry).typedProp<ElasticConfig>("config").all {
                prop(ElasticConfig::prefix).isEqualTo("elasticsearch")
                prop(ElasticConfig::host).isEqualTo("http://localhost:9200")
                prop(ElasticConfig::userName).isNull()
                prop(ElasticConfig::password).isNull()
                prop(ElasticConfig::index).isEqualTo("qalipsis-meters")
                prop(ElasticConfig::indexDateFormat).isEqualTo("yyyy-MM-dd")
                prop(ElasticConfig::autoCreateIndex).isEqualTo(true)
                prop(ElasticConfig::timestampFieldName).isEqualTo("@timestamp")
                prop(ElasticConfig::step).isEqualTo(Duration.ofSeconds(10))
            }

            generatedMeterRegistry = meterRegistryFactory.getRegistry(
                object : MeterRegistryConfiguration {
                    override val step: Duration = Duration.ofSeconds(3)

                }
            )
            assertThat(generatedMeterRegistry).typedProp<ElasticConfig>("config").all {
                prop(ElasticConfig::prefix).isEqualTo("elasticsearch")
                prop(ElasticConfig::host).isEqualTo("http://localhost:9200")
                prop(ElasticConfig::userName).isNull()
                prop(ElasticConfig::password).isNull()
                prop(ElasticConfig::index).isEqualTo("qalipsis-meters")
                prop(ElasticConfig::indexDateFormat).isEqualTo("yyyy-MM-dd")
                prop(ElasticConfig::autoCreateIndex).isEqualTo(true)
                prop(ElasticConfig::timestampFieldName).isEqualTo("@timestamp")
                prop(ElasticConfig::step).isEqualTo(Duration.ofSeconds(3))
            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.elasticsearch.enabled" to StringUtils.TRUE,
                "meters.export.elasticsearch.index" to "qalipsis-meters"
            )
        }
    }
}