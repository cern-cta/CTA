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

#include <algorithm>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "PluginInterfaces.hpp"

/** Macro PLUGIN_EXPORT_C makes a symbol visible. */
#if defined(_WIN32)
// MS-Windows NT
#define PLUGIN_EXPORT_C extern "C" __declspec(dllexport)
#else
// Unix-like OSes
#define PLUGIN_EXPORT_C extern "C" __attribute__((visibility("default")))
#endif

/** @brief Concrete implementation of the interface IPluginFactory.
 * An instance of this class is supposed to be exported by the plugin
 * entry-point function
 */
class PluginFactory : public IPluginFactory {
  /** Contains the pair <ClassName, Constructor Function  **/
  using CtorItem = std::pair<std::string, std::function<void *()>>;
  // Constructor database
  using CtorDB = std::vector<CtorItem>;

  /** Plugin name */
  std::string m_name;
  /** Plugin version */
  std::string m_version;
  CtorDB m_ctordb;

public:
  PluginFactory(const char *name, const char *version)
      : m_name(name), m_version(version) {}

  /** Get Plugin Name */
  const char *Name() const { return m_name.data(); }

  /** Get Plugin Version */
  const char *Version() const { return m_version.data(); }

  /** Get number of classes exported by the plugin */
  virtual size_t NumberOfClasses() const { return m_ctordb.size(); }
  virtual const char *GetClassName(size_t index) const {
    return m_ctordb[index].first.data();
  }

  /** Instantiate a class from its name.
   * This member function returns a type-erased pointer
   * to a class object allocated on the heap.
   */
  void *Factory(const char *className) const {
    auto it = std::find_if(
        m_ctordb.begin(), m_ctordb.end(),
        [&className](const auto &p) { return p.first == className; });
    if (it == m_ctordb.end())
      return nullptr;
    return it->second();
  }

  /** Register class name and constructor in the plugin database */
  template <typename AClass>
  PluginFactory &registerClass(std::string const &name) {
    auto constructor = [] { return new (std::nothrow) AClass; };
    m_ctordb.push_back(std::make_pair(name, constructor));
    return *this;
  }
};
