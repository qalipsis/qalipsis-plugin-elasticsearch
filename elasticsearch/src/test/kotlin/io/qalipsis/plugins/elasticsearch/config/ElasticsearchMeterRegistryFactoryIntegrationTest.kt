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
import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_7_IMAGE
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@Testcontainers
internal class ElasticsearchMeterRegistryFactoryIntegrationTest {

    @Nested
    @MicronautTest(startApplication = false, propertySources = ["classpath:application-elasticsearch.yml"])
    inner class NoMicronautElasticMeterRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "micronaut.metrics.export.elastic.enabled" to "false"
            )
        }

        @Test
        @Timeout(10)
        internal fun `should disables the default elaticsearch meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticMeterRegistry::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false)
    inner class WithoutMeters : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "micronaut.metrics.export.elastic.enabled" to "false",
                "meters.enabled" to "false",
                "meters.elasticsearch.enabled" to "true",
                "meters.elasticsearch.hosts" to CONTAINER.httpHostAddress
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
    @MicronautTest(startApplication = false)
    inner class WithMetersButWithoutElasticsearch : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "micronaut.metrics.export.elastic.enabled" to "false",
                "meters.enabled" to "true",
                "meters.elasticsearch.enabled" to "false",
                "meters.elasticsearch.hosts" to CONTAINER.httpHostAddress
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
    @MicronautTest(startApplication = false)
    inner class WithElasticsearchMeterRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "micronaut.metrics.export.elastic.enabled" to "false",
                "meters.enabled" to "true",
                "meters.elasticsearch.enabled" to "true",
                "meters.elasticsearch.hosts" to CONTAINER.httpHostAddress
            )
        }

        @Test
        @Timeout(10)
        internal fun `should start with ES meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticMeterRegistry::class.java)).hasSize(1)
        }
    }

    companion object {

        @Container
        @JvmStatic
        private val CONTAINER = ElasticsearchContainer(
            DockerImageName.parse(ELASTICSEARCH_7_IMAGE)
        ).withCreateContainerCmdModifier {
            it.hostConfig!!.withMemory((512 * 1e20).toLong()).withCpuCount(2)
        }
            .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
    }
}