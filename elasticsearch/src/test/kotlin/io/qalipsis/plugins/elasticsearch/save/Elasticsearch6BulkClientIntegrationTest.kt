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
            ElasticsearchContainer(DockerImageName.parse(ELASTICSEARCH_6_IMAGE)
        ).withCreateContainerCmdModifier {
            it.hostConfig!!.withMemory((512 * 1e20).toLong()).withCpuCount(2)
        }
            .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
    }
}
