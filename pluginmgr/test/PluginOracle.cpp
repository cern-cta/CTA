#include <iostream>
#include <cmath>

#include "PluginFactory.hpp"

#include "IDBMS.hpp"

class Oracle : public IDBMS {
public:
  const char* Name() const { return "Oracle"; }

  int Query(std::string str) const {
    std::cout << "Oracle Query: " << str << std::endl;
    return 0;
  }
};

// ===== Factory Function - Plugin EntryPoint ==== //

PLUGIN_EXPORT_C
auto GetPluginFactory() -> IPluginFactory* {
  static PluginFactory pinfo = [] {
    auto p = PluginFactory("PluginOracle", "0.1-alpha");
    p.registerClass<Oracle>("Oracle");
    return p;
  }();
  return &pinfo;
}

// struct _DLLInit {
//   _DLLInit() { std::cerr << " [TRACE] Shared library PluginOracle loaded OK." << std::endl; }

//   ~_DLLInit() { std::cerr << " [TRACE] Shared library PluginOracle unloaded OK." << std::endl; }
// } dll_init;
