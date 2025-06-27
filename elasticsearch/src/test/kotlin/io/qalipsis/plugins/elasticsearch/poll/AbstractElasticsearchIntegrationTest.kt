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
