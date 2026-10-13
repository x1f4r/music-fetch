pluginManagement {
    repositories {
        google {
            content {
                includeGroupByRegex("com\\.android.*")
                includeGroupByRegex("com\\.google.*")
                includeGroupByRegex("androidx.*")
            }
        }
        // Chaquopy's Gradle plugin is published to Maven Central.
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    // Resolves the JDK 21 daemon toolchain (gradle/gradle-daemon-jvm.properties) when it is not installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "MusicFetch"

include(":core", ":app")
