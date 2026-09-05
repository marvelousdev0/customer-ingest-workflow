import org.gradle.process.CommandLineArgumentProvider

plugins {
    java
    id("org.springframework.boot") version "4.1.1"
    id("io.spring.dependency-management") version "1.1.7"
    id("org.sonarqube") version "7.3.1.8318"
    id("com.diffplug.spotless") version "8.10.1"
}

group = "com.acme"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(26)
    }
}

repositories {
    mavenCentral()
    maven {
        name = "confluent"
        url = uri("https://packages.confluent.io/maven/")
    }
}

val avroVersion = "1.12.2"
val confluentVersion = "8.3.0"
val testcontainersVersion = "2.0.5"

val avroTools = configurations.create("avroTools")

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-mongodb")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-opentelemetry")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")

    implementation("org.apache.commons:commons-pool2")
    implementation("io.opentelemetry:opentelemetry-extension-trace-propagators")

    implementation("org.apache.avro:avro:$avroVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")

    avroTools("org.apache.avro:avro-tools:$avroVersion")

    testImplementation("org.springframework.boot:spring-boot-starter-actuator-test")
    testImplementation("org.springframework.boot:spring-boot-starter-data-mongodb-test")
    testImplementation("org.springframework.boot:spring-boot-starter-data-redis-test")
    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testImplementation("org.springframework.boot:spring-boot-starter-opentelemetry-test")
    testImplementation("org.springframework.boot:spring-boot-starter-validation-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-mongodb:$testcontainersVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Compile-time only: .avsc files are never packaged. Generated SpecificRecord
// POJOs (CustomerIngestEvent, Address, CustomerProcessedEvent) are the runtime types.
val generatedAvroDir = layout.buildDirectory.dir("generated-main-avro-java")

val generateAvroJava = tasks.register<JavaExec>("generateAvroJava") {
    group = "build"
    description = "Generate Avro SpecificRecord POJOs from src/main/avro (compile-time only; no runtime .avsc)"
    classpath = avroTools
    mainClass.set("org.apache.avro.tool.Main")
    outputs.dir(generatedAvroDir)
    inputs.dir(layout.projectDirectory.dir("src/main/avro"))
    argumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "compile",
                "-string",
                "schema",
                file("src/main/avro/Address.avsc").absolutePath,
                file("src/main/avro/CustomerIngestEvent.avsc").absolutePath,
                file("src/main/avro/CustomerProcessedEvent.avsc").absolutePath,
                generatedAvroDir.get().asFile.absolutePath,
            )
        },
    )
}

sourceSets {
    main {
        java {
            srcDir(generatedAvroDir)
        }
    }
}

spotless {
    java {
        target("src/*/java/**/*.java")
        googleJavaFormat("1.30.0").reflowLongStrings().formatJavadoc(true)
        formatAnnotations()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

tasks.register("format") {
    group = "formatting"
    description = "Format the full repo with Spotless (all Java under src/*/java)"
    dependsOn("spotlessApply")
}

val installGitHooks by tasks.registering {
    group = "build setup"
    description = "Install the Spotless pre-commit hook (format staged Java, restage into the same commit)"
    val hookSource = layout.projectDirectory.file("gradle/githooks/pre-commit")
    inputs.file(hookSource)
    doLast {
        val gitMarker = layout.projectDirectory.file(".git").asFile
        if (!gitMarker.exists()) {
            return@doLast
        }
        val process = ProcessBuilder("git", "rev-parse", "--git-path", "hooks")
            .directory(layout.projectDirectory.asFile)
            .redirectErrorStream(true)
            .start()
        val hooksRel = process.inputStream.bufferedReader().readText().trim()
        check(process.waitFor() == 0) { "git rev-parse --git-path hooks failed: $hooksRel" }
        val hooksDir = layout.projectDirectory.dir(hooksRel).asFile
        hooksDir.mkdirs()
        val target = hooksDir.resolve("pre-commit")
        hookSource.asFile.copyTo(target, overwrite = true)
        target.setExecutable(true, false)
    }
}

tasks.named("compileJava") {
    dependsOn(generateAvroJava, installGitHooks)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

// Generated Avro SpecificRecords are compile-only output and would drown a quality
// gate in codegen findings. Hand-written sources under src/main/java are analyzed.
sonar {
    properties {
        property("sonar.projectKey", "customer-ingest-workflow")
        property("sonar.projectName", "Customer Ingest Workflow")
        property("sonar.sources", "src/main/java")
        property("sonar.tests", "src/test/java")
        property("sonar.java.source", "26")
        property("sonar.sourceEncoding", "UTF-8")
        property("sonar.exclusions", "**/avro/**")
    }
}
