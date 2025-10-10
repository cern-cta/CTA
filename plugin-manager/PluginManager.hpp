/*
 * SPDX-FileCopyrightText: 2024 CERN
 * SPDX-FileCopyrightText: 2024 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "PluginInterface.hpp"

#include <stdexcept>
#include <string>
#include <memory>
#include <unordered_map>
#include <functional>
#include <dlfcn.h>

namespace cta::plugin {

class Loader {

public:
  Loader() = default;

  ~Loader() {
    unload();
  }

  Loader& load(const std::string& strFile) {
    m_strFile = strFile;
    if (m_pHandler) {
      throw std::logic_error("An attempt to load a library before unloading the previous one.");
    }
    // RTLD_NOW ensures that all the symbols are resolved immediately. This means that
	  // if a symbol cannot be found, the program will crash now instead of later.
    // RTLD_LAZY -
    m_pHandler = dlopen(strFile.c_str(), RTLD_NOW);

    if (!m_pHandler) {
      throw std::runtime_error(dlerror());
    }
    return *this;
  }

  Loader& unload() {
    if (m_pHandler) {
      dlclose(m_pHandler);
      m_pHandler = nullptr;
      m_strFile = "";
    }
    return *this;
  }

  Loader& attach(const std::string& strEntryPointName) {
    if (!m_pHandler) {
      throw std::runtime_error("Null pointer exception.");
    }

    dlerror(); // to clear any old error conditions
    m_pFun = dlsym(m_pHandler, strEntryPointName.c_str());
    char* pcError = dlerror();
    if (pcError) {
      throw std::runtime_error(pcError);
    }
    return *this;
  }

  const std::string& File() {
    return m_strFile;
  }

  template<typename HANDLER, typename RETURN, typename... ARGS>
  RETURN call(ARGS&... args) {
    if (!m_pFun) {
      throw std::runtime_error("Null pointer exception: an entry funcrion is not attached.");
    }
    HANDLER pFun = reinterpret_cast<HANDLER>(m_pFun);
    return pFun(args...);
  }

private:

  void* m_pHandler = nullptr;
  void* m_pFun = nullptr;
  std::string m_strFile;

};

template <typename BASE_TYPE, typename... IARGS>
class Manager {

public:

  ~Manager() {
    unloadAll();
  }

  bool isRegistered(const std::string& strPluginName) const {
    return m_umapPlugins.find(strPluginName) != m_umapPlugins.end();
  }

  void onRegisterPlugin(std::function<void (const plugin::Interface<BASE_TYPE, IARGS...>& )>&& callBackOnRegisterPlugin) {
    m_callBackOnRegisterPlugin = callBackOnRegisterPlugin;
  }

  void registerPlugin(std::unique_ptr<plugin::Interface<BASE_TYPE, IARGS...>> upInterface) {
    const std::string strPluginName = upInterface->template GET<plugin::DATA::PLUGIN_NAME>();
    if (isRegistered(strPluginName)) {
      throw std::logic_error("A plugin with the name: " + strPluginName + " is already registered.");
    }
    if (m_callBackOnRegisterPlugin) {
      m_callBackOnRegisterPlugin(*upInterface);
    }
    m_umapPlugins.emplace(strPluginName, std::move(upInterface));
  }

  const plugin::Interface<BASE_TYPE, IARGS...>& plugin(const std::string& strPluginName) const {
    if (!isRegistered(strPluginName)) {
      throw std::logic_error("A plugin with the name: " + strPluginName + " is not registered.");
    }
    return *m_umapPlugins.at(strPluginName);
  }

  std::vector<std::reference_wrapper<const std::unique_ptr<plugin::Interface<BASE_TYPE, IARGS...>>>> plugins() {
    std::vector<std::reference_wrapper<const std::unique_ptr<plugin::Interface<BASE_TYPE, IARGS...>>>> vPlugins;

    for (auto& iter : m_umapPlugins) {
      vPlugins.push_back(std::cref<std::unique_ptr<plugin::Interface<BASE_TYPE, IARGS...>>>(iter.second));
    }
    return vPlugins;
  }

  Manager& load(const std::string& strFile) {
    m_strActiveLoader = strFile;
    if (m_umapLoaders.find(m_strActiveLoader) == m_umapLoaders.end()) {
      m_umapLoaders.emplace(m_strActiveLoader, Loader {});
      m_umapLoaders.at(m_strActiveLoader).load(strFile);
    }
    return *this;
  }

  Manager& unloadAll() {
    m_umapPlugins.clear();
    m_umapLoaders.clear();
    return *this;
  }

  Manager& unload(const std::string& strFile) {
    for (auto iter = m_umapPlugins.begin() ; iter != m_umapPlugins.end(); ) {
      if(iter->second->template GET<plugin::DATA::FILE_NAME>() == strFile) {
        iter = m_umapPlugins.erase(iter);
      } else {
        iter++;
      }
    }
    m_umapLoaders.erase(strFile);
    return *this;
  }

  template<typename... ARGS>
  Manager&  bootstrap(const std::string& strEntryPoint, ARGS&... args) {
    loader().attach(strEntryPoint);
    std::unique_ptr<plugin::Interface<BASE_TYPE, IARGS...>> upInterface = std::make_unique<plugin::Interface<BASE_TYPE, IARGS...>>();

    loader().template call<void (*)(plugin::Interface<BASE_TYPE, IARGS...>&), void>(*upInterface, args...);
    // Set / overload plugin file name
    upInterface->template SET<plugin::DATA::FILE_NAME>(m_strActiveLoader);

    // Try to register the plugin
    registerPlugin(std::move(upInterface));

    return *this;
  }

private:
  std::string m_strActiveLoader;
  std::unordered_map<std::string, plugin::Loader> m_umapLoaders;
  std::unordered_map<std::string, std::unique_ptr<plugin::Interface<BASE_TYPE, IARGS...>>> m_umapPlugins;
  std::function<void (const plugin::Interface<BASE_TYPE, IARGS...>& )> m_callBackOnRegisterPlugin;

  Loader& loader() {
    try {
      return m_umapLoaders.at(m_strActiveLoader);
    } catch(const std::out_of_range& oor) {
      throw std::runtime_error("Loader " + m_strActiveLoader + " does not exists.");
    }
  }

};

} // namespace cta::plugin
