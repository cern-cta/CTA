#include <iostream>

#include "PluginLoader.hpp"

#include "IDBMS.hpp"

int main(int argc, char* argv[]) {

  // By default use the Oracle plugin, alternatively give "PluginPostgreSQL" as argument
  std::string pluginLib = "PluginOracle";
  if (argc == 2) {
    pluginLib = argv[1];
  }

  PluginManager ma;
  IPluginFactory* infoA = ma.addPlugin(pluginLib);
  assert(infoA != nullptr);

  std::cout << " ---- Plugin Information --------- "
            << "\n"
            << "  =>              Name = " << infoA->Name() << "\n"
            << "  =>           Version = " << infoA->Version() << "\n"
            << "  => Number of classes = " << infoA->NumberOfClasses() << "\n\n";

  std::cout << "Classes exported by the Plugin: (" << pluginLib << ") "
            << "\n";

  for (size_t n = 0; n < infoA->NumberOfClasses(); n++) {
    std::cout << " -> Exported Class: " << infoA->GetClassName(n) << "\n";
  }

  // Use DB via plugin interface IDBMS
  auto pDB = ma.CreateInstanceAs<IDBMS>(pluginLib, infoA->GetClassName(0));
  assert(pDB != nullptr);

  std::cout << "pDB->Name()    = " << pDB->Name() << "\n";
  std::cout << "pDB->Query(\"SELECT (*) AGE\") = " << pDB->Query("SELECT (*) AGE") << "\n";

  return 0;
}
