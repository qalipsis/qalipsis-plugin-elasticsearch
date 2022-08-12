package io.qalipsis.plugins.elasticsearch.events

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.config.EventsConfig
import io.qalipsis.api.constraints.PositiveDuration
import io.qalipsis.api.events.EventLevel
import java.time.Duration
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

/**
 * Configuration for [ElasticsearchEventsPublisher].
 *
 * @property minLevel minimal accepted level of events defaults to INFO.
 * @property urls list of URLs to the Elasticsearch instances defaults to http://localhost:9200.
 * @property indexPrefix prefix to use for the created indices containing the events defaults to qalipsis-events.
 * @property refreshInterval delay to refresh the indices in ES, defaults to 10s.
 * @property storeSource stores the source of the documents and not just the values, defaults to false.
 * @property indexDatePattern format of the date part of the index as supported by [java.time.format.DateTimeFormatter] defaults to uuuu-MM-dd to create an index per day.
 * @property lingerPeriod maximal period between two publication of events to Elasticsearch defaults to 10 seconds.
 * @property batchSize maximal number of events buffered between two publications of events to Elasticsearch defaults to 2000.
 * @property publishers number of concurrent publication of events that can be run defaults to 1 (no concurrency).
 * @property username name of the user to use for basic authentication when connecting to Elasticsearch.
 * @property password password of the user to use for basic authentication when connecting to Elasticsearch.
 * @property shards number of shards to apply on the created indices for events, defaults to 1.
 * @property replicas number of replicas to apply on the created indices for events, defaults to 0.
 * @property proxy URL of the http proxy to use to access to Elasticsearch it might be convenient in order to support other kind of authentication in Elasticsearch.
 *
 * @author Eric Jessé
 */
@Requires(property = "${EventsConfig.EXPORT_CONFIGURATION}.elasticsearch.enabled", value = "true")
@ConfigurationProperties("${EventsConfig.EXPORT_CONFIGURATION}.elasticsearch")
internal class ElasticsearchEventsConfiguration {

    @field:NotNull
    var minLevel: EventLevel = EventLevel.INFO

    @field:NotEmpty
    var urls: List<@NotBlank String> = listOf("http://localhost:9200")

    @field:NotBlank
    var pathPrefix: String = "/"

    @field:NotBlank
    var indexPrefix: String = "qalipsis-events"

    @field:NotBlank
    var refreshInterval: String = "10s"

    var storeSource: Boolean = false

    @field:NotBlank
    var indexDatePattern: String = "yyyy-MM-dd"

    @field:PositiveDuration
    var lingerPeriod: Duration = Duration.ofSeconds(10)

    @field:Min(1)
    var batchSize: Int = 40000

    @field:Min(1)
    var publishers: Int = 1

    var username: String? = null

    var password: String? = null

    @field:Min(1)
    var shards: Int = 1

    @field:Min(0)
    var replicas: Int = 0

    var proxy: String? = null
}
