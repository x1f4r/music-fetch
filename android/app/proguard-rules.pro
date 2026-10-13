# JNA and the UniFFI bindings are kept by :core's consumer rules; repeated here so
# the app stays correct if the core module's rules change.
-keep class com.sun.jna.** { *; }
-keep class * implements com.sun.jna.** { *; }
-keep class app.musicfetch.core.** { *; }
-dontwarn java.awt.**

# Chaquopy ships its own rules for com.chaquo.python. Kotlin types that Python
# code calls by name must keep their names and members, and their implementations
# must not be merged into one class (mf_ytdlp.py calls report() / isCanceled()).
-keep interface app.musicfetch.platform.PythonCallbacks$* { *; }
-keep class * implements app.musicfetch.platform.PythonCallbacks$* { *; }
