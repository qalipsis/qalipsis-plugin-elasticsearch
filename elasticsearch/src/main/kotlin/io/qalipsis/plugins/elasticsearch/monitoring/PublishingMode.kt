package io.qalipsis.plugins.elasticsearch.monitoring

/**
 * Defines the type of data to be exported into Elasticsearch.
 */
enum class PublishingMode(val value: String) {
    METERS("meters"),
    EVENTS("events")
}