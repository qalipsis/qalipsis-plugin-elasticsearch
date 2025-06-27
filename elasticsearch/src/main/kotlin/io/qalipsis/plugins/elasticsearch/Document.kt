/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
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