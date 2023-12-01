/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <cstdio>
#include <optional>
#include <sstream>
#include <string.h>

namespace cta::log {

/**
 * A name/value parameter for the CASTOR logging system.
 */
class Param {

public:

  /**
   * Constructor.
   *
   * @param name The name of the parameter.
   * @param value The value of the parameter that will be converted to a string
   * using std::ostringstream.
   */
  template <typename T> Param(const std::string &name, const T &value) noexcept:
    m_name(name) {
    std::ostringstream oss;
    oss << value;
    m_value = oss.str();
  }

  Param(const std::string & name, const uint8_t & value) noexcept:
  m_name(name) {
    std::ostringstream oss;
    oss << static_cast<int>(value);
    m_value = oss.str();
  }

  /**
   * Constructor.
   *
   * @param name The name of the parameter.
   * @param value The value of the parameter that will be converted to a string
   * using snprintf for doubles
   */
  Param (const std::string &name, const double value) noexcept:
  m_name(name) {
    char buf[1024];
    std::snprintf(buf, sizeof(buf), "%f", value);
    // Just in case we overflow
    buf[sizeof(buf)-1]='\0';
    m_value = buf;
  }

  Param(const std::string &name, const std::nullopt_t &value) noexcept:
    m_name(name) {
    m_value = "";
  }

  /**
   * Value changer. Useful for log contexts.
   * @param value
   */
  template <typename T>
  void setValue (const T &value) noexcept {
    std::stringstream oss;
    oss << value;
    m_value = oss.str();
  }

  /**
   * Returns a const reference to the name of the parameter.
   */
  const std::string &getName() const noexcept;

  /**
   * Returns a const reference to the value of the parameter.
   */
  const std::string &getValue() const noexcept;

protected:

  /**
   * Name of the parameter
   */
  std::string m_name;

  /**
   * The value of the parameter.
   */
  std::string m_value;

}; // class Param

/**
 * An helper class allowing the construction of a Param class with sprintf
 * formatting for a double.
 */
class ParamDoubleSnprintf: public Param {
public:
  ParamDoubleSnprintf(const std::string &name, const double value);
}; // class ParamDoubleSnprintf

} // namespace cta::log
