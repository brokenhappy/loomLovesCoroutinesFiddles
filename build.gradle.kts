import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.4.0"
}

group = "dev.wout"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(27)
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_26)
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-preview", "--add-opens=java.base/java.lang=ALL-UNNAMED")
}

tasks.withType<JavaExec>().configureEach {
    jvmArgs("--enable-preview", "--add-opens=java.base/java.lang=ALL-UNNAMED")
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(26)
}
