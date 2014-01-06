/******************************************************************************
 *                      Param.hpp
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
 * A parameter for the CASTOR log system
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_LOG_PARAM_HPP
#define CASTOR_LOG_PARAM_HPP 1

#include "castor/IObject.hpp"
#include "castor/log/IPAddress.hpp"
#include "castor/log/TimeStamp.hpp"
#include "h/Cuuid.h"

#include <string.h>
#include <stdlib.h>

namespace castor {
namespace log {

/**
 * A parameter for the CASTOR logging system.
 */
class Param {

public:

  /**
   * Constructor for strings.
   */
  Param(const std::string &name, const std::string &value) throw();

  /**
   * Constructor for uuids.
   */
  Param(const std::string &name, const Cuuid_t value) throw();

  /**
   * Constructor for SubRequest uuids.
   */
  Param(const Cuuid_t &value) throw();

  /**
   * Constructor for long int.
   */
  Param(const std::string &name, const long int value) throw();

  /**
   * Constructor for long unsigned int.
   */
  Param(const std::string &name, const long unsigned int value) throw();

  /**
   * Constructor for int.
   */
  Param(const std::string &name, const int value) throw();

  /**
   * Constructor for unsigned int.
   */
  Param(const std::string &name, const unsigned int value) throw();

  /**
   * Constructor for u_signed64.
   */
  Param(const std::string &name, const u_signed64 value) throw();

  /**
   * Constructor for floats.
   */
  Param(const std::string &name, const float value) throw();

  /**
   * Constructor for doubles.
   */
  Param(const std::string &name, const double value) throw();

  /**
   * Constructor for Raw parameters.
   */
  Param(const std::string &rawParams) throw();

  /**
   * Constructor for IPAddress.
   */
  Param(const std::string &name, const castor::log::IPAddress &value) throw();

  /**
   * Constructor for TimeStamp.
   */
  Param(const std::string &name, const castor::log::TimeStamp &value) throw();

  /**
   * Constructor for objects.
   */
  Param(const std::string &name, const castor::IObject *const value) throw();

  /**
   * Returns a const reference to the name of the parameter.
   */
  const std::string &getName() const throw();

  /**
   * Returns the type of the parameter.
   */
  int getType() const throw();

  /**
   * Returns a const refverence to the string value if there is one, else an
   * empty string.
   */
  const std::string &getStrValue() const throw();

  /**
   * Returns the int value if there is one, else 0.
   */
  int getIntValue() const throw();

  /**
   * Returns the unsigned 64-bit int value if there is one, else 0.
   */
  u_signed64 getUint64Value() const throw();

  /**
   * Returns the double value if there is one, else 0.0;
   */
  double getDoubleValue() const throw();

  /**
   * Returns a const reference to the UUID value if there is one, else
   * nullCuuid.
   */
  const Cuuid_t &getUuidValue() const throw();

private:

  /**
   * Name of the parameter
   */
  std::string m_name;

  /**
   * Parameter type, one of LOG_MSG_PARAM_*.
   */
  int m_type;

  /**
   * The string value of the parameter.
   */
  std::string m_strValue;

  /**
   * The int value of the parameter.
   */
  int m_intValue;

  /**
   * The unsigned 64-bit int value of the parameter.
   */
  u_signed64 m_uint64Value;

  /**
   * The double value of the parameter.
   */
  double m_doubleValue;

  /**
   * The UUID value of the parameter.
   */
  Cuuid_t m_uuidValue;

  /**
   * Empty string constant used by the getStrValue() method in the case where
   * m_type does not equal LOG_MSG_PARAM_STR.
   */
  const std::string m_emptyStr;

}; // class Param

} // namespace log
} // namespace castor

#endif // CASTOR_LOG_PARAM_HPP
