# JNA binds native methods and callback structures reflectively by name.
-keep class com.sun.jna.** { *; }
-keep class * implements com.sun.jna.** { *; }
-dontwarn java.awt.**

# UniFFI: JNA direct-mapped library interfaces, callback vtables and FFI converters
# are looked up by name from native code.
-keep class app.musicfetch.core.** { *; }
