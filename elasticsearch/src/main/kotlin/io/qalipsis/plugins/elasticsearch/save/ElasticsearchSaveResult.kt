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

package io.qalipsis.plugins.elasticsearch.save

import io.qalipsis.plugins.elasticsearch.Document
import io.qalipsis.plugins.elasticsearch.ElasticsearchBulkResponse

/**
 * Wrapper for the result of save records procedure in Elasticsearch.
 *
 * @property input the data to save in Elasticsearch
 * @property documentsToSave documents to be saved
 * @property responseBody response to the Elasticsearch response
 * @property meters meters of the save step
 *
 * @author Alex Averyanov
 */
data class ElasticsearchSaveResult<I>(
    val input: I,
    val documentsToSave: List<Document>,
    val responseBody: ElasticsearchBulkResponse?,
    val meters: ElasticsearchBulkMeters
)
