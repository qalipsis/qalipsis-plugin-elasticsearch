/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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