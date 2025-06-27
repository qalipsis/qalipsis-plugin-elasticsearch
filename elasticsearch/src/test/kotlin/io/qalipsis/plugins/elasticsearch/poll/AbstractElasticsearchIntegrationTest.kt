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

package io.qalipsis.plugins.elasticsearch.poll

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient
import org.junit.Assert
import java.util.UUID

/**
 *
 * @author Eric Jessé
 */
internal abstract class AbstractElasticsearchIntegrationTest {

    protected val jsonMapper = JsonMapper().also {
        it.registerModule(JavaTimeModule())
        it.registerModule(KotlinModule.Builder().build())
        it.registerModule(Jdk8Module())
    }

    protected fun createIndex(restClient: RestClient, index: String, indexConfiguration: String = "") {
        val request = Request("PUT", "/${index}")
        request.setJsonEntity(indexConfiguration)
        val responseBody = EntityUtils.toString(restClient.performRequest(request).entity)
        val response = jsonMapper.readTree(responseBody).get("acknowledged").booleanValue()
        Assert.assertTrue("An error occurred while creating the index: $responseBody", response)
    }

    protected fun bulk(restClient: RestClient, index: String, documents: List<String>, withType: Boolean) {
        val request = Request("POST", "/_bulk")
        request.addParameter("refresh", "true")
        val bulk = documents.joinToString("\n", postfix = "\n") {
            val metadata = if (withType) {
                """{ "create" : { "_index" : "$index", "_type": "_doc", "_id" : "${UUID.randomUUID()}" } }"""
            } else {
                """{ "create" : { "_index" : "$index", "_id" : "${UUID.randomUUID()}" } }"""
            }
            metadata + "\n" + it
        }
        request.setJsonEntity(bulk)
        val responseBody = EntityUtils.toString(restClient.performRequest(request).entity)
        val response = jsonMapper.readTree(responseBody)
        Assert.assertFalse("Errors occurred during the bulk request: $responseBody",
                response.get("errors").booleanValue())
    }

    protected fun count(restClient: RestClient, index: String): Int {
        val request = Request("GET", "/${index}/_count")
        return jsonMapper.readTree(EntityUtils.toByteArray(restClient.performRequest(request).entity)).get("count")
            .intValue()
    }

}
