package io.qalipsis.plugins.elasticsearch.events

import io.qalipsis.plugins.elasticsearch.ELASTICSEARCH_7_IMAGE
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName
import java.time.format.DateTimeFormatter

internal class Elasticsearch7EventsPublisherIntegrationTest : AbstractElasticsearchEventsPublisherIntegrationTest() {

    override val container: ElasticsearchContainer = CONTAINER

    override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

    override val requiresType: Boolean = false

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
