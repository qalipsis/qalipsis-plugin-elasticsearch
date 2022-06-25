plugins {
    kotlin("jvm")
    kotlin("kapt")
    kotlin("plugin.allopen")
}

description = "Qalipsis Plugins - Elasticsearch"

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

val micronautVersion: String by project
val kotlinCoroutinesVersion: String by project
val testContainersVersion: String by project
val jacksonVersion: String by project

val elasticsearchVersion = "7.12.0"
val catadioptreVersion: String by project

kotlin.sourceSets["test"].kotlin.srcDir("build/generated/source/kaptKotlin/catadioptre")
kapt.useBuildCache = false

dependencies {
    compileOnly("io.aeris-consulting:catadioptre-annotations:${catadioptreVersion}")
    compileOnly(kotlin("stdlib"))
    compileOnly(platform("io.micronaut:micronaut-bom:$micronautVersion"))

    compileOnly("io.micronaut:micronaut-runtime")
    compileOnly("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    api("org.elasticsearch.client:elasticsearch-rest-client:$elasticsearchVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")

    api("io.qalipsis:api-common:${project.version}")
    api("io.qalipsis:api-dsl:${project.version}")
    api("io.micronaut.micrometer:micronaut-micrometer-registry-elastic")

    kapt("io.aeris-consulting:catadioptre-annotations:${catadioptreVersion}")
    kapt(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    kapt("io.micronaut:micronaut-inject-java")
    kapt("io.micronaut:micronaut-validation")
    kapt("io.micronaut:micronaut-graal")
    kapt("io.qalipsis:api-processors:${project.version}")
    kapt("io.qalipsis:api-dsl:${project.version}")
    kapt("io.qalipsis:api-common:${project.version}")

    testImplementation("io.aeris-consulting:catadioptre-kotlin:${catadioptreVersion}")
    testImplementation("org.testcontainers:elasticsearch:$testContainersVersion")
    testImplementation("io.micronaut.test:micronaut-test-junit5")
    testImplementation("io.qalipsis:test:${project.version}")
    testImplementation("io.qalipsis:api-dsl:${project.version}")
    testImplementation(testFixtures("io.qalipsis:api-dsl:${project.version}"))
    testImplementation(testFixtures("io.qalipsis:api-common:${project.version}"))
    testImplementation(testFixtures("io.qalipsis:runtime:${project.version}"))
    testImplementation("javax.annotation:javax.annotation-api")
    testImplementation("io.micronaut:micronaut-runtime")
    testRuntimeOnly("io.qalipsis:runtime:${project.version}")
    testRuntimeOnly("io.qalipsis:head:${project.version}")
    testRuntimeOnly("io.qalipsis:factory:${project.version}")


    kaptTest(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    kaptTest("io.micronaut:micronaut-inject-java")
    kaptTest("io.qalipsis:api-processors:${project.version}")
}


