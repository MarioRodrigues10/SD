/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java application project to get you started.
 * For more details on building Java & JVM projects, please refer to https://docs.gradle.org/8.10.1/userguide/building_java_projects.html in the Gradle documentation.
 * This project uses @Incubating APIs which are subject to change.
 */

plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    implementation(libs.guava)
    implementation("org.jfree:jfreechart:1.5.3")
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    mainClass = "com.group15.kvserver.Client"
}

tasks.register<JavaExec>("client") {
    group = "application"
    description = "Runs client"

    mainClass.set("com.group15.kvserver.Client")
    classpath = sourceSets["main"].runtimeClasspath

    standardInput = System.`in`
}

tasks.register<JavaExec>("server") {
    group = "application"
    description = "Runs server"

    mainClass.set("com.group15.kvserver.Server")
    classpath = sourceSets["main"].runtimeClasspath

    doFirst {
        if (project.hasProperty("args")) {
            args = (project.property("args") as String).split(",")
        } else {
            logger.warn("No arguments passed! Using defaults: [1, 1, 1]")
            args = listOf("10", "40", "1")
        }
    }
}

tasks.register<JavaExec>("tests") {
    group = "application"
    description = "Run tests"

    mainClass.set("com.group15.kvservertests.Runner")

    classpath = files(
        sourceSets["main"].runtimeClasspath,
        sourceSets["test"].runtimeClasspath
    )

    standardInput = System.`in`
}