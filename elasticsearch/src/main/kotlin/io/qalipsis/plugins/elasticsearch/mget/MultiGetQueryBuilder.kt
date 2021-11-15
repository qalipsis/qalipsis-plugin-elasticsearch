package io.qalipsis.plugins.elasticsearch.mget

/**
 * Builder of a multi get request.
 *
 * @author Eric Jessé
 */
interface MultiGetQueryBuilder {

    /**
     * Describes a single document to fetch. Successive calls to [doc] will fetch several documents in a single request.
     *
     * @param index the name of the index containing the document
     * @param id the unique document ID
     * @param type the name of the type containing the document, required for Elasticsearch 6 and below
     * @param routing the key for the primary shard the document resides on, required if routing is used during indexing
     * @param source if false, excludes all _source fields, defaults to true
     * @param storedFields the stored fields you want to retrieve, defaults to none
     * @param sourceInclude the fields to extract and return from the _source field, defaults to all
     * @param sourceExclude the fields to exclude from the returned _source field, defaults to none
     */
    fun doc(index: String, id: String, type: String? = null, routing: String? = null, source: Boolean = true,
            storedFields: List<String> = emptyList(),
            sourceInclude: List<String> = emptyList(),
            sourceExclude: List<String> = emptyList())

}