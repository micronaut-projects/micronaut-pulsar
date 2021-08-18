plugins {
    id("org.jetbrains.kotlin.jvm") version "1.4.32"
    id("org.jetbrains.kotlin.kapt") version "1.4.32"
    id("com.github.johnrengelman.shadow") version "7.0.0"
    id("io.micronaut.application") version "1.5.4"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.4.32"
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
    runtimeOnly("ch.qos.logback:logback-classic")
    compileOnly("jakarta.inject:jakarta.inject-api:2.0.0")
    implementation("io.micronaut:micronaut-validation")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.4.3")
    implementation(project(":pulsar"))
    implementation("io.projectreactor:reactor-core:3.4.8")

    kaptTest("io.micronaut:micronaut-inject-java")
    testImplementation("org.testcontainers:junit-jupiter:1.16.0")
    testImplementation("org.testcontainers:pulsar:1.16.0")

    runtimeOnly("com.fasterxml.jackson.module:jackson-module-kotlin")

}


application {
    mainClass.set("kotlinexample.ApplicationKt")
}
java {
    sourceCompatibility = JavaVersion.toVersion("1.8")
}

tasks {
    compileKotlin {
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }
    compileTestKotlin {
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }


}
