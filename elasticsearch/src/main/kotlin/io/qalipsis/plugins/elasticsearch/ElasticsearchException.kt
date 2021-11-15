package io.qalipsis.plugins.elasticsearch

/**
 *
 * Generic exception for unexpected failures in the Elasticsearch operations.
 *
 * @author Eric Jessé
 */
class ElasticsearchException(message: String) : RuntimeException(message)