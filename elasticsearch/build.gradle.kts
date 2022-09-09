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

plugins {
    kotlin("jvm")
    kotlin("kapt")
    kotlin("plugin.allopen")
}

description = "QALIPSIS plugin for Elasticsearch"

allOpen {
    annotations(
        "io.micronaut.aop.Around",
        "jakarta.inject.Singleton",
        "io.qalipsis.api.annotations.StepConverter",
        "io.qalipsis.api.annotations.StepDecorator",
        "io.qalipsis.api.annotations.PluginComponent",
        "io.qalipsis.api.annotations.Spec",
        "io.micronaut.validation.Validated"
    )
}

kotlin.sourceSets["test"].kotlin.srcDir("build/generated/source/kaptKotlin/catadioptre")
kapt.useBuildCache = false

val coreVersion: String by project
val elasticsearchVersion = "8.4.1"

dependencies {
    implementation(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    compileOnly("io.aeris-consulting:catadioptre-annotations")

    compileOnly("io.micronaut:micronaut-runtime")
    api("org.elasticsearch.client:elasticsearch-rest-client:$elasticsearchVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

    api("io.qalipsis:api-common")
    api("io.qalipsis:api-dsl")
    api("io.micronaut.micrometer:micronaut-micrometer-registry-elastic")

    kapt(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    kapt("io.aeris-consulting:catadioptre-annotations")
    kapt("io.micronaut:micronaut-inject-java")
    kapt("io.micronaut:micronaut-validation")
    kapt("io.micronaut:micronaut-graal")
    kapt("io.qalipsis:api-processors")
    kapt("io.qalipsis:api-dsl")
    kapt("io.qalipsis:api-common")

    testImplementation(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    testImplementation("io.aeris-consulting:catadioptre-kotlin")
    testImplementation("org.testcontainers:elasticsearch")
    testImplementation("io.micronaut.test:micronaut-test-junit5")
    testImplementation("io.qalipsis:test")
    testImplementation("io.qalipsis:api-dsl")
    testImplementation(testFixtures("io.qalipsis:api-dsl"))
    testImplementation(testFixtures("io.qalipsis:api-common"))
    testImplementation(testFixtures("io.qalipsis:runtime"))
    testImplementation("javax.annotation:javax.annotation-api")
    testImplementation("io.micronaut:micronaut-runtime")
    testRuntimeOnly("io.qalipsis:runtime")
    testRuntimeOnly("io.qalipsis:head")
    testRuntimeOnly("io.qalipsis:factory")


    kaptTest(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    kaptTest("io.micronaut:micronaut-inject-java")
    kaptTest("io.qalipsis:api-processors")
}


