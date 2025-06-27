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

package io.qalipsis.plugins.elasticsearch.save

import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_6_IMAGE
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName
import java.time.format.DateTimeFormatter

internal class Elasticsearch6BulkClientIntegrationTest : AbstractElasticsearchBulkClientIntegrationTest() {

    override val container: ElasticsearchContainer = CONTAINER

    override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

    override val requiresType: Boolean = true

    companion object {

        @Container
        @JvmStatic
        private val CONTAINER =
            ElasticsearchContainer(DockerImageName.parse(ELASTICSEARCH_6_IMAGE))
                .withCreateContainerCmdModifier {
                    it.hostConfig!!.withMemory((512 * 1e20).toLong()).withCpuCount(2)
                }
                .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
    }
}
