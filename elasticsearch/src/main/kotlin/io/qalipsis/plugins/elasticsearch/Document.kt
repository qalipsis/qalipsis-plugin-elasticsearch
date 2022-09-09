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