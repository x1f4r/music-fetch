plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.compose)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.chaquopy)
}

// Release signing comes from the environment (scripts/build-android-app.sh reads the
// password from the login keychain). Without it the release APK is left unsigned.
val releaseKeystore = providers.environmentVariable("MF_KEYSTORE_PATH").orNull
val releaseStorePassword = providers.environmentVariable("MF_KEYSTORE_PASSWORD").orNull
val releaseKeyAlias = providers.environmentVariable("MF_KEY_ALIAS").orNull
val releaseKeyPassword = providers.environmentVariable("MF_KEY_PASSWORD").orNull ?: releaseStorePassword
val canSignRelease = listOf(releaseKeystore, releaseStorePassword, releaseKeyAlias).none { it.isNullOrEmpty() }

android {
    namespace = "app.musicfetch"
    compileSdk = 37
    ndkVersion = "29.0.14206865"

    defaultConfig {
        applicationId = "app.musicfetch"
        minSdk = 29
        targetSdk = 36
        versionCode = 110
        versionName = "1.1.0"

        ndk {
            abiFilters += androidAbis()
        }
    }

    signingConfigs {
        if (canSignRelease) {
            create("release") {
                storeFile = file(releaseKeystore!!)
                storePassword = releaseStorePassword
                keyAlias = releaseKeyAlias
                keyPassword = releaseKeyPassword
                // v3 allows rotating the signing key later.
                enableV3Signing = true
            }
        }
    }

    buildTypes {
        debug {
            // Development files (microphone replay, melody-index manifest override); see DevFiles.
            buildConfigField("boolean", "DEV_TOOLS", "true")
        }
        release {
            isMinifyEnabled = true
            isShrinkResources = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            signingConfig = signingConfigs.findByName("release")
            buildConfigField("boolean", "DEV_TOOLS", "false")
        }
        // Release code and optimizations, profileable and with the development files, signed
        // with the debug key: for measuring speed and memory (`assembleBenchmark`).
        create("benchmark") {
            initWith(getByName("release"))
            signingConfig = signingConfigs.getByName("debug")
            matchingFallbacks += "release"
            isProfileable = true
            buildConfigField("boolean", "DEV_TOOLS", "true")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    buildFeatures {
        compose = true
        buildConfig = true
    }

    testOptions {
        // android.util.Log and friends return defaults in JVM unit tests.
        unitTests.isReturnDefaultValues = true
    }

    packaging {
        jniLibs {
            // Extract native libraries to nativeLibraryDir so libqjs.so (quickjs-ng,
            // used by yt-dlp) can be executed.
            useLegacyPackaging = true
        }
    }
}

chaquopy {
    defaultConfig {
        version = "3.13"
        buildPythonPath()?.let { buildPython(it) }
        pip {
            // Pinned; brotli and pycryptodomex come from Chaquopy's wheel index.
            install("yt-dlp==2026.8.19")
            install("yt-dlp-ejs==0.8.0")
            install("certifi==2026.7.22")
            install("brotli==1.1.0")
            install("pycryptodomex==3.21.0")
        }
        // Read through importlib.resources / file paths: keep them as real files.
        extractPackages("certifi", "yt_dlp_ejs")
    }
}

/**
 * ABIs to package: `-Pmusicfetch.abis=arm64-v8a,x86_64` (the default). Chaquopy ships Python
 * 3.13 for these two only; x86_64 is for the emulator. Must match the libmfcore.so builds.
 */
fun androidAbis(): List<String> =
    providers.gradleProperty("musicfetch.abis").orNull
        ?.split(',')?.map { it.trim() }?.filter { it.isNotEmpty() }
        ?: listOf("arm64-v8a", "x86_64")

/** A Python 3.13 for Chaquopy's build steps: MF_BUILD_PYTHON, or a known install location. */
fun buildPythonPath(): String? {
    providers.environmentVariable("MF_BUILD_PYTHON").orNull?.let { return it }
    return listOf(
        "/Library/Frameworks/Python.framework/Versions/3.13/bin/python3.13",
        "/opt/homebrew/bin/python3.13",
        "/usr/local/bin/python3.13",
        "/usr/bin/python3.13",
    ).firstOrNull { File(it).canExecute() }
}

dependencies {
    implementation(project(":core"))

    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.activity.compose)
    implementation(libs.androidx.lifecycle.runtime.compose)
    implementation(libs.androidx.lifecycle.viewmodel.compose)
    implementation(libs.androidx.lifecycle.viewmodel.navigation3)
    implementation(libs.androidx.navigation3.runtime)
    implementation(libs.androidx.navigation3.ui)
    implementation(libs.androidx.datastore.preferences)
    implementation(libs.androidx.lifecycle.process)
    implementation(libs.androidx.graphics.shapes)
    implementation(libs.androidx.glance.appwidget)
    implementation(libs.androidx.glance.material3)
    implementation(libs.androidx.work.runtime.ktx)

    implementation(platform(libs.compose.bom))
    implementation(libs.compose.ui)
    implementation(libs.compose.ui.graphics)
    implementation(libs.compose.ui.tooling.preview)
    implementation(libs.compose.material3)
    implementation(libs.compose.material3.navigation.suite)
    implementation(libs.materialkolor)
    debugImplementation(libs.compose.ui.tooling)

    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.coil.compose)
    implementation(libs.coil.network.okhttp)

    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
}
