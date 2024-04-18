/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2024 CERN
 * @copyright    Copyright © 2024 DESY
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <stdexcept>
#include <type_traits>
#include <memory>
#include <functional>
#include <unordered_map>
#include <string>

namespace cta::plugin {

enum class DATA {
  FILE_NAME = 0,
  API_VERSION,
  PLUGIN_NAME,
  PLUGIN_VERSION
};

template<typename... ARGS>
struct Base {
  virtual ~Base() = default;

  virtual void operator()(const ARGS&...) = 0;
};

template<typename BASE_TYPE>
class Interface {

public:

  template <plugin::DATA D>
  Interface& SET(const std::string& strValue) {
    m_umapData.emplace(D, strValue);
    return *this;
  }

  template <plugin::DATA D>
  const std::string& GET() const {
    return m_umapData.at(D);
  }

  template<typename TYPE>
  Interface& CLASS(const std::string& strClassName) {
    if (!std::is_base_of<BASE_TYPE, TYPE>::value) { 
      throw std::invalid_argument("plugin type not supported");
    }
    m_umapFactories.emplace(strClassName, []() -> std::unique_ptr<TYPE> { return std::make_unique<TYPE>(); });
    return *this;
  }

  std::unique_ptr<BASE_TYPE> create(std::string strClassName) const {
    return m_umapFactories.at(strClassName)();
  }

private:
  std::unordered_map<DATA, std::string> m_umapData;
  std::unordered_map<std::string, std::function<std::unique_ptr<BASE_TYPE> ()>> m_umapFactories;
};

}// namespace cta::plugin

