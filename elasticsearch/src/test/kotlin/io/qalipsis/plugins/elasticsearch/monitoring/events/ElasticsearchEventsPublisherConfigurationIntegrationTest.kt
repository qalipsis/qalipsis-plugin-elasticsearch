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

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.containsOnly
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventsPublisher
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration

/**
 * Tests to verify the automatic configuration of the Elasticsearch publisher.
 */
class ElasticsearchEventsPublisherConfigurationIntegrationTest {

    @Nested
    @MicronautTest(propertySources = ["classpath:application-elasticsearch.yml"])
    inner class NoPublisher {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should start without publisher`() {
            assertThat(applicationContext.getBeansOfType(EventsPublisher::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(ElasticsearchEventsConfiguration::class.java)).isEmpty()
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
            assertThat(applicationContext.getBeansOfType(EventsPublisher::class.java)).all {
                hasSize(1)
                any { it.isInstanceOf(ElasticsearchEventsPublisher::class) }
            }
            val configuration = applicationContext.getBean(ElasticsearchEventsConfiguration::class.java)
            assertThat(configuration).all {
                prop(ElasticsearchEventsConfiguration::minLevel).isEqualTo(EventLevel.TRACE)
                prop(ElasticsearchEventsConfiguration::urls).all {
                    hasSize(2)
                    containsOnly("http://localhost:9200", "http://localhost:9201")
                }
                prop(ElasticsearchEventsConfiguration::pathPrefix).isEqualTo("/es/")
                prop(ElasticsearchEventsConfiguration::indexPrefix).isEqualTo("the-events")
                prop(ElasticsearchEventsConfiguration::refreshInterval).isEqualTo("1m")
                prop(ElasticsearchEventsConfiguration::indexDatePattern).isEqualTo("yyyy-MM")
                prop(ElasticsearchEventsConfiguration::lingerPeriod).isEqualTo(Duration.ofSeconds(30))
                prop(ElasticsearchEventsConfiguration::batchSize).isEqualTo(100)
                prop(ElasticsearchEventsConfiguration::publishers).isEqualTo(3)
                prop(ElasticsearchEventsConfiguration::username).isEqualTo("the-user")
                prop(ElasticsearchEventsConfiguration::password).isEqualTo("the-password")
                prop(ElasticsearchEventsConfiguration::shards).isEqualTo(3)
                prop(ElasticsearchEventsConfiguration::replicas).isEqualTo(1)
                prop(ElasticsearchEventsConfiguration::proxy).isEqualTo("http://localhost:4000")
            }

        }
    }

}
