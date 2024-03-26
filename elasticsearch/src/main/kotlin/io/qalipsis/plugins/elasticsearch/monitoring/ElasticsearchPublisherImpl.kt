/*
 * Copyright 2024 AERIS IT Solutions GmbH
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

package io.qalipsis.plugins.elasticsearch.monitoring

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.sync.ImmutableSlot
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
import jakarta.inject.Singleton
import java.time.Duration
import java.util.*
import java.util.regex.Pattern
import kotlinx.coroutines.runBlocking
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseListener
import org.elasticsearch.client.RestClient
import kotlin.coroutines.CoroutineContext

/**
 * Implementation of [ElasticsearchPublisher] to handle initialization of elasticsearch templates, as well as seeding data into elasticsearch.
 *
 * @author Francisca Eze
 */
@Singleton
internal class ElasticsearchPublisherImpl : ElasticsearchPublisher {

    private lateinit var restClient: RestClient

    private var majorVersionIsSevenOrMore = false

    private val jsonMapper: ObjectMapper = ObjectMapper()

    private val random = Random()

    private lateinit var elasticSearchConfiguration: MonitoringConfiguration

    override fun buildClient(configuration: MonitoringConfiguration) {
        elasticSearchConfiguration = configuration
        val builder = RestClient.builder(*configuration.urls.map { HttpHost.create(it) }.toTypedArray())
        builder.setPathPrefix(configuration.pathPrefix)
        configuration.proxy?.let {
            builder.setHttpClientConfigCallback { asyncBuilder ->
                if (!configuration.username.isNullOrBlank() && !configuration.password.isNullOrBlank()) {
                    asyncBuilder.setDefaultCredentialsProvider(
                        BasicCredentialsProvider().also {
                            it.setCredentials(
                                AuthScope.ANY,
                                UsernamePasswordCredentials(configuration.username, configuration.password)
                            )
                        }
                    )
                }
                if (!configuration.proxy.isNullOrBlank()) {
                    asyncBuilder.setProxy(HttpHost.create(configuration.proxy))
                }
                asyncBuilder
            }
        }
        restClient = builder.build()
    }

    override fun initializeTemplate(configuration: MonitoringConfiguration, publishingMode: String) {
        logger.debug { "Checking the version of Elasticsearch" }
        val versionTree =
            jsonMapper.readTree(EntityUtils.toByteArray(restClient.performRequest(Request("GET", "/")).entity))
        val version = (versionTree.get("version")?.get("number") as TextNode).textValue()
        majorVersionIsSevenOrMore = version.substringBefore(".").toInt() >= 7
        logger.debug { "Using Elasticsearch $version" }

        val templateResource =
            if (majorVersionIsSevenOrMore) "$publishingMode/index-template-from-7.json" else "$publishingMode/index-template-before-7.json"
        val jsonTemplate = jsonMapper.readTree(this::class.java.classLoader.getResource(templateResource)) as ObjectNode
        val templateName = "qalipsis-$publishingMode"
        jsonTemplate.put("index_patterns", "${configuration.indexPrefix}-*")
        (jsonTemplate["aliases"] as ObjectNode).putObject(configuration.indexPrefix)
        (jsonTemplate["settings"] as ObjectNode).apply {
            put("number_of_shards", configuration.shards)
            put("number_of_replicas", configuration.replicas)
            (this["index"] as ObjectNode).put("refresh_interval", configuration.refreshInterval)
        }
        (if (majorVersionIsSevenOrMore) {
            (jsonTemplate["mappings"] as ObjectNode)
        } else {
            ((jsonTemplate["mappings"] as ObjectNode)["_doc"] as ObjectNode)
        }["_source"] as ObjectNode).put("enabled", configuration.storeSource)
        val putTemplateRequest = Request("PUT", "_template/$templateName")
        putTemplateRequest.setJsonEntity(jsonTemplate.toString())
        val templateCreationResponse = EntityUtils.toString(restClient.performRequest(putTemplateRequest).entity)
        if (templateCreationResponse.contains(ERROR_RESPONSE_BODY_SIGNATURE)) {
            logger.error { "Failed to create or update the index template $templateName in Elasticsearch" }
        } else {
            logger.debug { "Successfully created or updated the index template $templateName in Elasticsearch" }
        }
    }

    override fun createBulkItem(formattedDateToDocument: Pair<String, String>): String {
        val uuid = UUID(random.nextLong(), random.nextLong())
        val type = if (majorVersionIsSevenOrMore) "" else """"_type":"$DOCUMENT_TYPE","""
        return """
            {"index":{"_index":"${elasticSearchConfiguration.indexPrefix}-${formattedDateToDocument.first}",$type"_id":"$uuid"}}
            ${formattedDateToDocument.second}
        """.trimIndent()
    }

     override suspend fun executeBulk(
        bulkRequest: Request,
        exportStart: Long,
        numberOfSentItems: Int,
        meterRegistry: CampaignMeterRegistry?,
        coroutineContext: CoroutineContext,
        monitoringType: String,
    ) {
        val slot = ImmutableSlot<Result<Unit>>()
        restClient.performRequestAsync(bulkRequest, object : ResponseListener {
            override fun onSuccess(response: Response) {
                val exportEnd = System.nanoTime()
                val responseBody = EntityUtils.toString(response.entity)
                if (responseBody.contains(ERROR_RESPONSE_BODY_SIGNATURE)) {
                    meterRegistry?.timer(
                        scenarioName = "",
                        stepName = "",
                        name = "elasticsearch.$monitoringType.export",
                        tags = mapOf("publisher" to "elasticsearch", "status" to "failure")
                    )?.record(Duration.ofNanos(exportEnd - exportStart))
                    val numberOfCreatedItems = countCreatedItems(responseBody)
                    val errors = jsonMapper.readTree(responseBody)
                        .withArray<ObjectNode>("items")
                        .asSequence()
                        .map { it["index"] as ObjectNode } // Reads the index operation.
                        .filterNot { it["status"].asInt(400) == 201 } // Finds the ones with a status != 201.
                        .map { "Document ${it["_id"].asText()}: ${it["error"]}" }

                    logger.error {
                        "Failed to send $monitoringType to Elasticsearch (sent $numberOfSentItems $monitoringType but only created $numberOfCreatedItems): $responseBody\n${
                            errors.joinToString("\n\t\t", prefix = "\t\t")
                        }"
                    }
                    logger.debug { "Failed $monitoringType payload: ${bulkRequest.entity}" }
                    runBlocking(coroutineContext) {
                        slot.set(Result.failure(ElasticsearchException(responseBody)))
                    }
                } else {
                    meterRegistry?.timer(
                        scenarioName = "",
                        stepName = "",
                        name = "elasticsearch.$monitoringType.export",
                        tags = mapOf("publisher" to "elasticsearch", "status" to "success")
                    )?.record(Duration.ofNanos(exportEnd - exportStart))
                    logger.debug { "Successfully sent $numberOfSentItems $monitoringType to Elasticsearch" }
                    runBlocking(coroutineContext) {
                        slot.set(Result.success(Unit))
                    }
                }
                logger.trace { "onSuccess totally processed" }
            }

            override fun onFailure(exception: Exception) {
                runBlocking(coroutineContext) {
                    slot.set(Result.failure(exception))
                }
                logger.trace { "onFailure totally processed" }
            }
        })
        slot.get().getOrThrow()
    }


    /**
     * Copied from [io.micrometer.elastic.ElasticMeterRegistry.countCreatedItems].
     */
    @KTestable
    override fun countCreatedItems(responseBody: String): Int {
        val matcher = STATUS_CREATED_PATTERN.matcher(responseBody)
        var count = 0
        while (matcher.find()) {
            count++
        }
        return count
    }

    companion object {

        private const val DOCUMENT_TYPE = "_doc"

        @JvmStatic
        private val STATUS_CREATED_PATTERN = Pattern.compile("\"status\":201")

        @JvmStatic
        private val ERROR_RESPONSE_BODY_SIGNATURE = "\"errors\":true"

        @JvmStatic
        private val logger = logger()
    }

}