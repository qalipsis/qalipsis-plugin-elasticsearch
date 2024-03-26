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

package io.qalipsis.plugins.elasticsearch.monitoring.events

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventsPublisher
import io.qalipsis.plugins.elasticsearch.monitoring.events.ElasticsearchEventsConfiguration
import io.qalipsis.plugins.elasticsearch.monitoring.events.ElasticsearchEventsPublisher
import jakarta.inject.Inject
import java.time.Duration
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils.containsOnly

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
