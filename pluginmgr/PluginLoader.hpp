/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of
 * the GNU General Public Licence version 3 (GPL Version 3), copied verbatim in
 * the file "COPYING". You can redistribute it and/or modify it under the terms
 * of the GPL Version 3, or (at your option) any later version.
 *
 *               This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges
 * and immunities granted to it by virtue of its status as an Intergovernmental
 * Organization or submit itself to any jurisdiction.
 */

#pragma once

#include <cassert>
#include <map>
#include <memory>

#include "PluginInterfaces.hpp"

// Unix
#if defined(_WIN32)
#include <windows.h>
#elif defined(__unix__)
// APIs: dlopen, dlclose, dlopen
#include <dlfcn.h>
#elif defined(__APPLE__)
// APIs: dlopen, dlclose, dlopen
#include <dlfcn.h>
#else
#error "Not supported operating system"
#endif

#ifdef GetClassName
#undef GetClassName
#endif

/** @brief Class form managing and encapsulating shared libraries loading  */
class Plugin {
public:
  /** @brief Function pointer to DLL entry-point */
  using GetPluginInfo_fp = IPluginFactory *(*)();
  /** @brief Name of DLL entry point that a Plugin should export */
  static constexpr const char *DLLEntryPointName = "GetPluginFactory";

  /** @brief Shared library handle */
  void *m_hnd = nullptr;
  /** @brief Shared library file name */
  std::string m_file = "";
  /** @brief Flag to indicate whether plugin (shared library) is loaded into
   * current process. */
  bool m_isLoaded = false;
  /** @brief Pointer to shared library factory class returned by the DLL
   * entry-point function */
  IPluginFactory *m_info = nullptr;

  Plugin() {}

  explicit Plugin(std::string file) {
    m_file = std::move(file);
#if defined(__unix__)
    m_hnd = ::dlopen(m_file.c_str(), RTLD_LAZY);
#elif defined(__APPLE__)
    m_hnd = ::dlopen(m_file.c_str(), RTLD_LAZY);
    if (!m_hnd) {
      // try .so extension
      m_file = m_file.substr(0, m_file.find_last_of('.')) + ".so";
      m_hnd = ::dlopen(m_file.c_str(), RTLD_LAZY);
    }
#elif defined(_WIN32)
    m_hnd = (void *)::LoadLibraryA(m_file.c_str());
#endif
    if (!m_hnd)
       std::cout << "Failed loading plugin: " << m_file << std::endl;
    assert(m_hnd != nullptr);
    m_isLoaded = true;
#if !defined(_WIN32)
    auto dllEntryPoint =
        reinterpret_cast<GetPluginInfo_fp>(dlsym(m_hnd, DLLEntryPointName));
#else
    auto dllEntryPoint = reinterpret_cast<GetPluginInfo_fp>(
        GetProcAddress((HMODULE)m_hnd, DLLEntryPointName));
#endif
    assert(dllEntryPoint != nullptr);
    // Retrieve plugin metadata from DLL entry-point function
    m_info = dllEntryPoint();
  }

  ~Plugin() { this->Unload(); }
  Plugin(Plugin const &) = delete;
  Plugin &operator=(const Plugin &) = delete;

  Plugin(Plugin &&rhs) {
    m_isLoaded = std::move(rhs.m_isLoaded);
    m_hnd = std::move(rhs.m_hnd);
    m_file = std::move(rhs.m_file);
    m_info = std::move(rhs.m_info);
  }
  Plugin &operator=(Plugin &&rhs) {
    std::swap(rhs.m_isLoaded, m_isLoaded);
    std::swap(rhs.m_hnd, m_hnd);
    std::swap(rhs.m_file, m_file);
    std::swap(rhs.m_info, m_info);
    return *this;
  }

  IPluginFactory *GetInfo() const { return m_info; }

  void *CreateInstance(std::string const &className) {
    return m_info->Factory(className.c_str());
  }

  bool isLoaded() const { return m_isLoaded; }

  void Unload() {
    if (m_hnd != nullptr) {
#if !defined(_WIN32)
      ::dlclose(m_hnd);
#else
      ::FreeLibrary((HMODULE)m_hnd);
#endif
      m_hnd = nullptr;
      m_isLoaded = false;
    }
  }
};

/** @brief Repository of plugins.
 * It can instantiate any class from any loaded plugin by its name.
 **/
class PluginManager {
public:
  using PluginMap = std::map<std::string, Plugin>;
  // Plugins database
  PluginMap m_plugindb;

  PluginManager() {}

  std::string GetExtension() const {
    std::string ext;
#if defined(_WIN32)
    ext = ".dll"; // Windows
#elif defined(__unix__) && !defined(__APPLE__)
    ext = ".so"; // Linux, BDS, Solaris and so on.
#elif defined(__APPLE__)
    ext = ".dylib"; // MacOSX
#else
#error "Not implemented for this platform"
#endif
    return ext;
  }

  IPluginFactory *addPlugin(const std::string &name) {
    std::string fileName = name + GetExtension();
    m_plugindb[name] = Plugin(fileName);
    return m_plugindb[name].GetInfo();
  }

  IPluginFactory *GetPluginFactory(const char *pluginName) {
    auto it = m_plugindb.find(pluginName);
    if (it == m_plugindb.end())
      return nullptr;
    return it->second.GetInfo();
  }

  /** @brief Instantiate some class exported by some plugin.
   *  @details
   *  This member function returns a pointer to the object
   *  casted as void pointer.  It is responsibility of the caller to
   *  cast the return pointer to a proper interface implemented by
   *  the loaded class.
   **/
  void *CreateInstance(std::string pluginName, std::string className) {
    auto it = m_plugindb.find(pluginName);
    if (it == m_plugindb.end())
      return nullptr;
    return it->second.CreateInstance(className);
  }

  /** @brief Instantiate a class exported by some loaded plugin.
   *  @tparam T           Interface (interface class) implemented by the loaded
   * class.
   *  @param  pluginName  Name of the plugin that exports the class.
   *  @param  className   Name of the class exported by the plugin.
   *  @return             Instance of exported class casted to some interface T.
   * */
  template <typename T>
  std::shared_ptr<T> CreateInstanceAs(std::string pluginName,
                                      std::string className) {
    void *pObj = CreateInstance(std::move(pluginName), std::move(className));
    return std::shared_ptr<T>(reinterpret_cast<T *>(pObj));
  }
};
