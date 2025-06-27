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

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.TextNode
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient


internal object ElasticsearchUtility {

        /**
         * Utility method to check the version of elasticsearch.
         *
         * @param jsonMapper JSON mapper to interpret the results
         * @param restClient Elasticsearch rest client used to execute the request
         */
        fun checkElasticsearchVersionIsGreaterThanSeven(jsonMapper: JsonMapper, restClient: RestClient): Int {
            val versionTree =
                jsonMapper.readTree(EntityUtils.toByteArray(restClient.performRequest(Request("GET", "/")).entity))
            val version = (versionTree.get("version")?.get("number") as TextNode).textValue()
            return version.substringBefore(".").toInt()
        }

}