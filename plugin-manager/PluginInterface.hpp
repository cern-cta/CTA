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
#include <tuple>
#include <variant>
#include <unordered_map>
#include <string>

namespace cta::plugin {

enum class DATA {
  FILE_NAME = 0,
  API_VERSION,
  PLUGIN_NAME,
  PLUGIN_VERSION
};

template<typename... ARGS> using Args = std::tuple<ARGS...>;

template<typename BASE_TYPE, typename... IARGS>
class Interface {

public:

  template<plugin::DATA D>
  Interface& SET(const std::string& strValue) {
    m_umapData.emplace(D, strValue);
    return *this;
  }

  template<plugin::DATA D>
  const std::string& GET() const {
    if (m_umapData.find(D) == m_umapData.end()) {
      throw std::logic_error("Invalid access to the plugin data.");
    }
    return m_umapData.at(D);
  }

  template<typename TYPE>
  Interface& CLASS(const std::string& strClassName) {
    static_assert(std::is_base_of<BASE_TYPE, TYPE>::value, "Plugin type not supported.");
       
    m_umapFactories.emplace(strClassName, [](const std::variant<IARGS...>& varaintPluginArgs) -> std::unique_ptr<TYPE> {
      try {
        std::unique_ptr<TYPE> upType = std::visit([](auto&& tuplePluginArgs) ->
          std::unique_ptr<TYPE> {
          // unpack parameter pack (IARGS...) 
          return std::apply([](auto&&... unpackedPluginArgs) -> std::unique_ptr<TYPE> {
                   return make<TYPE>(unpackedPluginArgs...);
                 }, tuplePluginArgs);
        }, varaintPluginArgs);

        return upType;

      } catch (const std::bad_variant_access& e) {
        throw std::logic_error("Invalid plugin interface.");
      }
    });

    return *this;
  }

  template<typename... ARGS>
  constexpr
  std::unique_ptr<BASE_TYPE> make(const std::string& strClassName, ARGS&&... args) const {
    if (m_umapFactories.find(strClassName) == m_umapFactories.end()) {
      throw std::logic_error("The "
                              + GET<plugin::DATA::PLUGIN_NAME>()
                              + " plugin does not provide a class called: "
                              + strClassName);
    }
    return m_umapFactories.at(strClassName)(std::forward_as_tuple(args...));
  }

private:
  std::unordered_map<DATA, std::string> m_umapData;
  std::unordered_map<std::string, std::function<std::unique_ptr<BASE_TYPE> (const std::variant<IARGS...>&)>> m_umapFactories;

  /**
   * A static partial specialized template for instantiating a class
   * to be captured by the lambda function if a class can be constructible
   */
  template<typename TYPE, typename... ARGS>
  static std::enable_if_t<std::is_constructible_v<TYPE, ARGS&&...>, std::unique_ptr<TYPE>> make(ARGS&&... args) {
    return std::make_unique<TYPE>(args...);
  }
  /**
   * A static partial specialized template for instantiating a class
   * to be captured by the lambda function if a class cannot be constructible 
   */
  template<typename TYPE, typename... ARGS>
  static std::enable_if_t<!std::is_constructible_v<TYPE, ARGS&&...>, std::unique_ptr<TYPE>> make(ARGS&&... ) {
    throw std::logic_error("The class cannot be instantiated. No constructor has been implemented for the given parameter set.");
  }

};
 
} // namespace cta::plugin
