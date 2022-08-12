package io.qalipsis.plugins.elasticsearch.config

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isNotEmpty
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.elastic.ElasticMeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class ElasticsearchMeterRegistryFactoryIntegrationTest {

    @Nested
    @MicronautTest(startApplication = false, environments = ["elasticsearch"])
    inner class NoMicronautElasticMeterRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should disables the default ES meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticMeterRegistry::class.java)).isEmpty()
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
        internal fun `should start without ES meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticMeterRegistry::class.java)).isEmpty()
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
        internal fun `should start without ES meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticMeterRegistry::class.java)).isEmpty()
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
        internal fun `should start with ES meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticMeterRegistry::class.java)).hasSize(1)
        }
    }
}