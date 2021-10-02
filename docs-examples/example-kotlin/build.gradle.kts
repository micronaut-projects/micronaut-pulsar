plugins {
    id("org.jetbrains.kotlin.jvm") version "1.5.21"
    id("org.jetbrains.kotlin.kapt") version "1.5.21"
    id("com.github.johnrengelman.shadow") version "7.0.0"
    id("io.micronaut.application") version "1.5.4"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.5.21"
}

version = "0.1"
group = "kotlinexample"

val kotlinVersion= project.properties["kotlinVersion"]
repositories {
    mavenCentral()
}

micronaut {
    runtime("netty")
    testRuntime("kotest")
    processing {
        incremental(true)
        annotations("kotlinexample.*")
    }
}

dependencies {
    implementation("io.micronaut:micronaut-http-client")
    implementation("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("org.jetbrains.kotlin:kotlin-reflect:${kotlinVersion}")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:${kotlinVersion}")
    implementation("ch.qos.logback:logback-classic")
    compileOnly("jakarta.inject:jakarta.inject-api:2.0.0")
    implementation("io.micronaut:micronaut-validation")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.5.1")
    implementation(project(":pulsar"))
    implementation("io.projectreactor:reactor-core")

    kaptTest("io.micronaut:micronaut-inject-java")
    testImplementation("org.testcontainers:junit-jupiter:1.16.0")
    testImplementation("org.testcontainers:pulsar:1.16.0")

    runtimeOnly("com.fasterxml.jackson.module:jackson-module-kotlin")

}


application {
    mainClass.set("kotlinexample.ApplicationKt")
    // due to this being subproject and parent is ignoring child gradle.properties this is a workaround
    if (JavaVersion.VERSION_16 <= JavaVersion.current()) {
        applicationDefaultJvmArgs = applicationDefaultJvmArgs.plus("org.gradle.jvmargs=--illegal-access=permit")
    }
}

java {
    sourceCompatibility = if (JavaVersion.current() >= JavaVersion.VERSION_14) JavaVersion.toVersion("14")
    else JavaVersion.toVersion("1.8")
}

tasks {
    val version = when(JavaVersion.current()) {
        JavaVersion.VERSION_14 -> "14"
        JavaVersion.VERSION_11 -> "11"
        else -> "1.8"
    }
    compileKotlin {
        kotlinOptions {
            jvmTarget = version
            javaParameters = true
        }
    }
    compileTestKotlin {
        kotlinOptions {
            jvmTarget = version
            javaParameters = true
        }
    }
}
