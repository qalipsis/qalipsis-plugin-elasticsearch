package io.qalipsis.plugins.elasticsearch

/**
 * Entity to save in Elasticsearch.
 *
 * @property index Name of the index associated with the operation.
 * @property type  The document type associated with the operation. Elasticsearch indices now support a single document type: _doc.
 * @property id The document ID associated with the operation.
 * @property source data to be indexed.
 *
 * @author Alex Averyanov
 */
data class Document(
    val index: String,
    val type: String,
    val id: String?,
    val source: String
){
    constructor(index: String, source: String):this(index, "_doc", null, source)
    constructor(index: String, id: String, source: String):this(index, "_doc", id, source)
}