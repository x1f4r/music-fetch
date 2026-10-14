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

# Glance updates widgets through WorkManager 2.7, which creates its Room database
# (WorkDatabase_Impl) reflectively. Room 2.2's rule keeps RoomDatabase subclasses but not
# their constructors, which R8 full mode then removes and the app crashes at startup.
-keep class * extends androidx.room.RoomDatabase { <init>(); }
