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

package io.qalipsis.plugins.elasticsearch.monitoring

import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_8_IMAGE
import io.qalipsis.plugins.elasticsearch.monitoring.meters.AbstractElasticsearchMeasurementPublisherIntegrationTest
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName
internal class Elasticsearch8MeasurementPublisherIntegrationTest : AbstractElasticsearchMeasurementPublisherIntegrationTest() {

    override val container: ElasticsearchContainer = CONTAINER

    override val requiresType: Boolean = false

    companion object {

        @Container
        @JvmStatic
        private val CONTAINER = ElasticsearchContainer(
            DockerImageName.parse(ELASTICSEARCH_8_IMAGE)
        ).withCreateContainerCmdModifier {
            it.hostConfig!!.withMemory((512 * 1e20).toLong()).withCpuCount(2)
        }
            .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
            .withEnv("action.destructive_requires_name", "false")
            .withEnv("xpack.security.enabled", "false")
    }
}