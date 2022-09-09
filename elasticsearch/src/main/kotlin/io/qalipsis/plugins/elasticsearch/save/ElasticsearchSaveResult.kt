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
