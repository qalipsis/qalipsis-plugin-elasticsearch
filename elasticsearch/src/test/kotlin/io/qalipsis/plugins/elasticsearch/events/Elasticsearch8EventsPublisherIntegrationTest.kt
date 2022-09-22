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

package io.qalipsis.plugins.elasticsearch.events

import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_8_IMAGE
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName
import java.time.format.DateTimeFormatter

internal class Elasticsearch8EventsPublisherIntegrationTest : AbstractElasticsearchEventsPublisherIntegrationTest() {

    override val container: ElasticsearchContainer = CONTAINER

    override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

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
