package io.qalipsis.plugins.elasticsearch

import io.qalipsis.plugins.elasticsearch.save.ElasticsearchBulkResult

class ElasticsearchSaveException(message: String, val response: ElasticsearchBulkResult) : RuntimeException(message)