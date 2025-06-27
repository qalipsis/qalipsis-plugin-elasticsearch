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

package io.qalipsis.plugins.elasticsearch.monitoring

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.sync.ImmutableSlot
import io.qalipsis.plugins.elasticsearch.ElasticsearchException
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
import java.time.Duration
import java.util.Random
import java.util.UUID
import java.util.regex.Pattern
import kotlin.coroutines.CoroutineContext

/**
 * Implementation of [ElasticsearchOperations] to handle initialization of elasticsearch templates, as well as exporting data into elasticsearch.
 *
 * @author Francisca Eze
 */
internal class ElasticsearchOperationsImpl : ElasticsearchOperations {

    /**
     * RestClient that connects to an Elasticsearch cluster through HTTP.
     */
    private lateinit var restClient: RestClient

    /**
     * Indicates if the version of the Elasticsearch client is version 7 or higher.
     */
    private var majorVersionIsSevenOrMore = false

    /**
     * Facilitates the serialization and deserialization of Java objects.
     */
    private val jsonMapper: ObjectMapper = ObjectMapper()

    private val random = Random()

    override fun buildClient(configuration: MonitoringConfiguration) {
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

    override fun initializeTemplate(configuration: MonitoringConfiguration, publishingMode: PublishingMode) {
        logger.debug { "Checking the version of Elasticsearch" }
        val versionTree =
            jsonMapper.readTree(EntityUtils.toByteArray(restClient.performRequest(Request("GET", "/")).entity))
        val version = (versionTree.get("version")?.get("number") as TextNode).textValue()
        majorVersionIsSevenOrMore = version.substringBefore(".").toInt() >= 7
        logger.debug { "Using Elasticsearch $version" }

        val templateResource =
            if (majorVersionIsSevenOrMore) "${publishingMode.value}/index-template-from-7.json" else "${publishingMode.value}/index-template-before-7.json"
        val jsonTemplate = jsonMapper.readTree(this::class.java.classLoader.getResource(templateResource)) as ObjectNode
        val templateName = "qalipsis-${publishingMode.value}"
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

    override fun createBulkItem(formattedDateToDocument: Pair<String, String>, indexPrefix: String): String {
        val uuid = UUID(random.nextLong(), random.nextLong())
        val type = if (majorVersionIsSevenOrMore) "" else """"_type":"$DOCUMENT_TYPE","""
        return """
            {"index":{"_index":"$indexPrefix-${formattedDateToDocument.first}",$type"_id":"$uuid"}}
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
    override fun countCreatedItems(responseBody: String): Int {
        val matcher = STATUS_CREATED_PATTERN.matcher(responseBody)
        var count = 0
        while (matcher.find()) {
            count++
        }
        return count
    }

    override fun close() {
        restClient.close()
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