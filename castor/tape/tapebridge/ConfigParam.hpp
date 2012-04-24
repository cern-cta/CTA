/******************************************************************************
 *                      castor/tape/tapebridge/ConfigParam.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAM_HPP
#define CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAM_HPP

#include "castor/exception/NoValue.hpp"
#include "castor/tape/tapebridge/ConfigParamSource.hpp"
#include "castor/tape/utils/utils.hpp"
#include <string>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Class used to store the name, value and source of a configuration parameter.
 */
template <typename ValueType> class ConfigParam {
public:

  /**
   * Constructor used to create a configuration parameter without its value
   * being and source being set.
   *
   * @param category The category name of the configuration parameter as used
   *                 in the castor.conf file.
   * @param name     The name of the configuration parameter as used in the
   *                 castor.conf file.
   */
  ConfigParam(
    const std::string &category,
    const std::string &name):
    m_category(category),
    m_name(name),
    m_source(ConfigParamSource::UNDEFINED),
    m_valueAndSourceAreSet(false) {
    // Do nothing
  }

  /**
   * Sets the value and source of the configuration parameter.
   *
   * @param value  The value of the configuration parameter.
   * @param source The source of the configuration parameter.
   */
  void setValueAndSource(
    const ValueType         &value,
    ConfigParamSource::Enum source) {
    m_value                = value;
    m_source               = source;
    m_valueAndSourceAreSet = true;
  }

  /**
   * Returns the category name of the configuration parameter as used in the
   * castor.conf file.
   */
  const std::string &getCategory() const throw() {
    return m_category;
  }

  /**
   * Returns the name of the configuration parameter as used in the castor.conf
   * file.
   */
  const std::string &getName() const throw() {
    return m_name;
  }

  /**
   * Returns the value of the configuration parameter.
   *
   * This method throws a castor::exception::NoValue exception if the value
   * and source of the configuration parameter have not been set.
   */
  ValueType getValue() const throw(castor::exception::NoValue) {
    if(!valueAndSourceAreSet()) {
      TAPE_THROW_EX(castor::exception::NoValue,
        ": Value and source of configuation parameter have not been set"
        ": category=" << getCategory() << " name=" << getName());
    }
    return m_value;
  }

  /**
   * Returns the source of the configuration parameter.
   *
   * This method throws a castor::exception::NoValue exception if the value
   * and source of the configuration parameter have not been set.
   */
  ConfigParamSource::Enum getSource() const
    throw(castor::exception::NoValue) {
    if(!valueAndSourceAreSet()) {
      TAPE_THROW_EX(castor::exception::NoValue,
        ": Value and source of configuation parameter have not been set"
        ": category=" << getCategory() << " name=" << getName());
    }
    return m_source;
  }

  /**
   * Returns true if the value and source of the configuration parameter have
   * been set.
   */
  bool valueAndSourceAreSet() const throw() {
    return m_valueAndSourceAreSet;
  }

private:

  /**
   * The category name of the configuration parameter as used in the
   * castor.conf file.
   */
  std::string m_category;

  /**
   * The name of the configuration parameter as used in the castor.conf file.
   */
  std::string m_name;

  /**
   * The value of the configuration parameter.
   */
  ValueType m_value;

  /**
   * The source of the configuration parameter.
   */
  ConfigParamSource::Enum m_source;

  /**
   * Has a value of true if the value and source of the configuration parameter
   * have been set.
   */
  bool m_valueAndSourceAreSet;

}; // class ConfigParam

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAM_HPP
