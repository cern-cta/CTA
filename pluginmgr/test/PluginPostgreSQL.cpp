#include <iostream>
#include <cmath>

#include "PluginFactory.hpp"

#include "IDBMS.hpp"

class PostgreSQL : public IDBMS {
public:
  const char* Name() const { return "PostgreSQL"; }

  int Query(std::string str) const {
    std::cout << "PostgreSQL Query: " << str << std::endl;
    return 0;
  }
};

// ===== Factory Function - Plugin EntryPoint ==== //

PLUGIN_EXPORT_C
auto GetPluginFactory() -> IPluginFactory* {
  static PluginFactory pinfo = [] {
    auto p = PluginFactory("PluginPostgreSQL", "0.2");
    p.registerClass<PostgreSQL>("PostgreSQL");
    return p;
  }();
  return &pinfo;
}

// struct _DLLInit {
//   _DLLInit() { std::cerr << " [TRACE] Shared library PluginPostgreSQL loaded OK." << std::endl; }

//   ~_DLLInit() { std::cerr << " [TRACE] Shared library PluginPostgreSQL unloaded OK." << std::endl; }
// } dll_init;
