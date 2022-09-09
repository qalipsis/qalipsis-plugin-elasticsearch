package io.qalipsis.plugins.elasticsearch.save

import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.ElasticsearchStepSpecification
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient

/**
 * Specification for a [ElasticsearchSaveStep] to save data to a Elasticsearch.
 *
 * @author Alex Averyanov
 */
@Spec
interface ElasticsearchSaveStepSpecification<I> :
    StepSpecification<I, ElasticsearchSaveResult<I>, ElasticsearchSaveStepSpecification<I>>,
    ElasticsearchStepSpecification<I, ElasticsearchSaveResult<I>, ElasticsearchSaveStepSpecification<I>>,
    ConfigurableStepSpecification<I, ElasticsearchSaveResult<I>, ElasticsearchSaveStepSpecification<I>> {

    /**
     * Configures the REST client to connect to Elasticsearch.
     */
    fun client(client: () -> RestClient)

    /**
     * Defines the statement to execute when saving.
     */
    fun documents(query: suspend (ctx: StepContext<*, *>, input: I) -> List<Document>)

    /**
     * Keep the Elasticsearch bulk response into the result of the step.
     */
    fun keepResponse()

    /**
     * Configures the monitoring of the save step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)

}

/**
 * Implementation of [ElasticsearchSaveStepSpecification].
 *
 * @author Alex Averyanov
 */
@Spec
internal class ElasticsearchSaveStepSpecificationImpl<I> :
    ElasticsearchSaveStepSpecification<I>,
    AbstractStepSpecification<I, ElasticsearchSaveResult<I>, ElasticsearchSaveStepSpecification<I>>() {

    internal var client: (() -> RestClient) = { RestClient.builder(HttpHost("localhost", 9200, "http")).build() }

    internal var documentsFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<Document> =
        { _, _ -> emptyList() }

    internal var keepResponse = false

    internal var monitoringConfig = StepMonitoringConfiguration()

    override fun client(client: () -> RestClient) {
        this.client = client
    }

    override fun documents(query: suspend (ctx: StepContext<*, *>, input: I) -> List<Document>) {
        this.documentsFactory = query
    }

    override fun keepResponse() {
        this.keepResponse = true
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }

}

/**
 * Saves documents into Elasticsearch.
 *
 * @author Alex Averyanov
 */
fun <I> ElasticsearchStepSpecification<*, I, *>.save(
    configurationBlock: ElasticsearchSaveStepSpecification<I>.() -> Unit
): ElasticsearchSaveStepSpecification<I> {
    val step = ElasticsearchSaveStepSpecificationImpl<I>()
    step.configurationBlock()

    this.add(step)
    return step
}