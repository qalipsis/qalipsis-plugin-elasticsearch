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