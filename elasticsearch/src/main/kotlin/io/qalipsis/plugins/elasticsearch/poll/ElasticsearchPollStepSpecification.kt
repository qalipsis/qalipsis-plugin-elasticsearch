package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.json.JsonMapper
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.BroadcastSpecification
import io.qalipsis.api.steps.LoopableSpecification
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.UnicastSpecification
import io.qalipsis.plugins.elasticsearch.ElasticsearchDocument
import io.qalipsis.plugins.elasticsearch.ElasticsearchScenarioSpecification
import io.qalipsis.plugins.elasticsearch.ElasticsearchSearchMetricsConfiguration
import io.qalipsis.plugins.elasticsearch.ElasticsearchStepSpecification
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.jetbrains.annotations.NotNull
import java.time.Duration
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty
import kotlin.reflect.KClass

/**
 * Specification for an [io.qalipsis.api.steps.datasource.IterativeDatasourceStep] to poll data from a Elasticsearch.
 *
 * The output is a list of [ElasticsearchDocument] contains maps of column names to values.
 *
 * When [flatten] is called, the records are provided one by one to the next step, otherwise each poll batch remains complete.
 *
 * @author Eric Jessé
 */
@Spec
interface ElasticsearchPollStepSpecification :
    StepSpecification<Unit, List<ElasticsearchDocument<Map<String, Any?>>>, PollDeserializable<Map<String, Any?>>>,
    ElasticsearchStepSpecification<Unit, List<ElasticsearchDocument<Map<String, Any?>>>, PollDeserializable<Map<String, Any?>>>,
    LoopableSpecification, UnicastSpecification, BroadcastSpecification {

    /**
     * Configures the REST client to connect to Elasticsearch.
     */
    fun client(client: () -> RestClient)

    /**
     * Configures the JSON Mapper to deserialize the records.
     */
    fun mapper(mapper: (JsonMapper) -> Unit)

    /**
     * Options to add as query parameters.
     *
     * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-search-api-query-params) for the values.
     *
     * @param param pairs of parameters names and values
     */
    fun queryParameters(vararg param: Pair<String, String>)

    /**
     * Elasticsearch indices to use for the queries. Defaults to "_all".
     *
     * @param index complete or wildcard index name
     */
    fun index(@NotBlank vararg index: String)

    /**
     * Builder for the JSON query to perform for first poll. Behind the scene we use Elasticsearch capability
     * for "search after" and the tie-breaker is extracted from the sort clauses.
     *
     * Defaults to all the documents in the target sorted by "_id".
     *
     * See the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after) to know the limitations.
     *
     * @param json a closure to create the JSON
     */
    fun query(@NotBlank json: () -> String)

    /**
     * Delay between two executions of poll.
     *
     * @param delay the delay to wait between the end of a poll and start of next one
     */
    fun pollDelay(delay: Duration)

    /**
     * Configures the metrics of the poll step.
     */
    fun metrics(metricsConfiguration: ElasticsearchSearchMetricsConfiguration.() -> Unit)
}

/**
 * Implementation of [ElasticsearchPollStepSpecification].
 *
 * @author Eric Jessé
 */
@Spec
internal class ElasticsearchPollStepSpecificationImpl :
    AbstractStepSpecification<Unit, List<ElasticsearchDocument<Map<String, Any?>>>, PollDeserializable<Map<String, Any?>>>(),
    PollDeserializable<Map<String, Any?>>, ElasticsearchPollStepSpecification {

    override val singletonConfiguration: SingletonConfiguration = SingletonConfiguration(SingletonType.UNICAST)

    internal var client: (() -> RestClient) = { RestClient.builder(HttpHost("localhost", 9200, "http")).build() }

    internal var mapper: ((JsonMapper) -> Unit) = { }

    @field:NotEmpty
    internal val indices: MutableList<@NotBlank String> = mutableListOf("_all")

    internal val queryParameters: MutableMap<@NotBlank String, String> = mutableMapOf()

    internal var queryFactory: (() -> String) = { """{"query":{"match_all":{}},"sort":"_id"}""" }

    @field:NotNull
    internal var pollDelay: Duration? = null

    internal val metrics = ElasticsearchSearchMetricsConfiguration()

    internal var flattenOutput = false

    internal var convertFullDocument = false

    internal var targetClass: KClass<*> = Map::class

    override fun client(client: () -> RestClient) {
        this.client = client
    }

    override fun mapper(mapper: (JsonMapper) -> Unit) {
        this.mapper = mapper
    }

    override fun queryParameters(vararg param: Pair<String, String>) {
        this.queryParameters.clear()
        this.queryParameters.putAll(param)
    }

    override fun index(vararg index: String) {
        indices.clear()
        indices.addAll(index.toList())
    }

    override fun query(json: () -> String) {
        queryFactory = json
    }

    override fun pollDelay(delay: Duration) {
        this.pollDelay = delay
    }

    override fun metrics(metricsConfiguration: ElasticsearchSearchMetricsConfiguration.() -> Unit) {
        this.metrics.metricsConfiguration()
    }

    override fun <O : Any> deserialize(targetClass: KClass<O>,
                                       fullDocument: Boolean): StepSpecification<Unit, List<ElasticsearchDocument<O>>, *> {
        convertFullDocument = fullDocument
        flattenOutput = false
        this.targetClass = targetClass

        @Suppress("UNCHECKED_CAST")
        return this as StepSpecification<Unit, List<ElasticsearchDocument<O>>, *>
    }

    override fun flatten(fullDocument: Boolean): StepSpecification<Unit, ElasticsearchDocument<Map<String, Any?>>, *> {
        convertFullDocument = fullDocument
        flattenOutput = true
        this.targetClass = Map::class

        @Suppress("UNCHECKED_CAST")
        return this as StepSpecification<Unit, ElasticsearchDocument<Map<String, Any?>>, *>
    }

    override fun <O : Any> flatten(targetClass: KClass<O>,
                                   fullDocument: Boolean): StepSpecification<Unit, ElasticsearchDocument<O>, *> {
        convertFullDocument = fullDocument
        flattenOutput = true
        this.targetClass = targetClass

        @Suppress("UNCHECKED_CAST")
        return this as StepSpecification<Unit, ElasticsearchDocument<O>, *>
    }
}

/**
 * Creates a Elasticsearch poll step in order to periodically fetch data from a Elasticsearch cluster.
 *
 * This step is generally used in conjunction with a left join to assert data or inject them in a workflow
 *
 * @author Eric Jessé
 */
fun ElasticsearchScenarioSpecification.poll(
        configurationBlock: ElasticsearchPollStepSpecification.() -> Unit
): PollDeserializable<Map<String, Any?>> {
    val step = ElasticsearchPollStepSpecificationImpl()
    step.configurationBlock()

    (this as StepSpecificationRegistry).add(step)
    return step
}
