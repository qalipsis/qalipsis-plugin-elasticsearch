package io.qalipsis.plugins.elasticsearch.mget

import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Implementation of [MultiGetQueryBuilderImpl] to be instantiated for each single execution of a multi get step.
 *
 * @author Eric Jessé
 */
internal class MultiGetQueryBuilderImpl : MultiGetQueryBuilder {

    private val documents = mutableListOf<DocumentIdentifier>()

    override fun doc(index: String, id: String, type: String?, routing: String?, source: Boolean,
                     storedFields: List<String>, sourceInclude: List<String>, sourceExclude: List<String>) {
        documents.add(DocumentIdentifier(index, type, id, routing, source, storedFields, sourceInclude, sourceExclude))
    }

    fun toJson(root: ObjectNode) {
        val array = root.putArray("docs")
        documents.forEach { doc ->
            val docNode = array.addObject()
            docNode.put("_index", doc.index)
            docNode.put("_id", doc.id)
            doc.type?.apply { docNode.put("_type", this) }
            doc.routing?.apply { docNode.put("_routing", this) }

            addCollectionIfNotEmpty(docNode, "_stored_fields", doc.storedFields)

            if (doc.sourceInclude.isEmpty() && doc.sourceExclude.isEmpty()) {
                docNode.put("_source", doc.source)
            } else {
                val sourceNode = docNode.putObject("_source")
                addCollectionIfNotEmpty(sourceNode, "include", doc.sourceInclude)
                addCollectionIfNotEmpty(sourceNode, "exclude", doc.sourceExclude)
            }
        }
    }

    private fun addCollectionIfNotEmpty(parentNode: ObjectNode, fieldName: String, values: Collection<String>) {
        if (values.isNotEmpty()) {
            val array = parentNode.putArray(fieldName)
            values.forEach(array::add)
        }
    }

    /**
     * Description of the identification of a document to fetch, as described in the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html#docs-multi-get-api-request-body).

     * @property index the name of the index containing the document
     * @property type the name of the type containing the document, required for Elasticsearch 6 and below
     * @property id the unique document ID
     * @property routing the key for the primary shard the document resides on, required if routing is used during indexing
     * @property source if false, excludes all _source fields, defaults to true
     * @property storedFields the stored fields you want to retrieve, defaults to none
     * @property sourceInclude the fields to extract and return from the _source field, defaults to all
     * @property sourceExclude the fields to exclude from the returned _source field, defaults to none
     *
     * @author Eric Jessé
     */
    private data class DocumentIdentifier(
            val index: String,
            val type: String?,
            val id: String,
            val routing: String? = null,
            val source: Boolean = true,
            val storedFields: List<String>,
            val sourceInclude: List<String>,
            val sourceExclude: List<String>
    )
}