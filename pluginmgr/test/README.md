* Simple C++ Plugin Manager 

A C++ header-only library for developing
plugin systems that allow loading classes dynamically from plugins
(shared libraries) at runtime. 

Features: 

 + A shared library plugin file (*.dll on Windows or *.so on Linux or
   any other Unix-like OS) can export classes matching
   interfaces known by the plugin loader.

 + Able to load any class matching some known interface class
   known by the plugin (implementation) and client code (code that
   loads the plugin).

 + Third-parties can buid new plugin components without having to
   compile the main application or having its source code. 

 + Header-only library

 + Cross-platform

 + Plugin metadata

Tested at: 
 + Linux
 + macOS
 + Windows NT (Windows 10) with MSVC compiler

To test do:
 + make; ./main [PluginOracle | PluginPostgreSQL]

 or

 + mkdir b; cd b; cmake ..; make; main.bin [PluginOracle | PluginPostgreSQL]
