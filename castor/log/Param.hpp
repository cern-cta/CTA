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
 * @author castor dev team
 *****************************************************************************/

#ifndef CASTOR_LOG_PARAM_HPP
#define CASTOR_LOG_PARAM_HPP 1

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/TapeVid.hpp"
#include "castor/log/IPAddress.hpp"
#include "castor/log/TimeStamp.hpp"
#include "h/Cuuid.h"

#include <string.h>
#include <stdlib.h>

/* Parameter types */
#define LOG_MSG_PARAM_DOUBLE  1     /* Double precision floating point value */
#define LOG_MSG_PARAM_INT64   2     /* 64 bit integers */
#define LOG_MSG_PARAM_STR     3     /* General purpose string */
#define LOG_MSG_PARAM_TPVID   4     /* Tape visual identifier string */
#define LOG_MSG_PARAM_UUID    5     /* Subrequest identifier */
#define LOG_MSG_PARAM_FLOAT   6     /* Single precision floating point value */
#define LOG_MSG_PARAM_INT     7     /* Integer parameter */
#define LOG_MSG_PARAM_UID     8
#define LOG_MSG_PARAM_GID     9
#define LOG_MSG_PARAM_STYPE   10
#define LOG_MSG_PARAM_SNAME   11
#define LOG_MSG_PARAM_RAW     12    /* raw (set of) parameters, in key=value format */

namespace castor {
namespace log {

/**
 * A parameter for the CASTOR logging system.
 */
class Param {

public:

  /**
   * Constructor for strings
   */
  Param(const std::string &name, const std::string &value) :
    m_name(name),
    m_type(name == "TPVID" ? LOG_MSG_PARAM_TPVID : LOG_MSG_PARAM_STR),
    m_strValue(value), m_intValue(0), m_uint64Value(0), m_doubleValue(0.0),
    m_uuidValue(nullCuuid) {
  }

  /**
   * Constructor for uuids
   */
  Param(const std::string &name, const Cuuid_t value) :
    m_name(name), m_type(LOG_MSG_PARAM_UUID),
    m_strValue(), m_intValue(0), m_uint64Value(0), m_doubleValue(0.0),
    m_uuidValue(value) {
  }

  /**
   * Constructor for SubRequest uuids
   */
  Param(const Cuuid_t value) :
    m_name(""), m_type(LOG_MSG_PARAM_UUID),
    m_strValue(), m_intValue(0), m_uint64Value(0), m_doubleValue(0.0), 
    m_uuidValue(value) {
  }

  /**
   * Constructor for long int
   */
#if defined __x86_64__
  Param(const std::string &name, const long int value) :
    m_name(name), m_type(LOG_MSG_PARAM_INT64),
    m_strValue(), m_intValue(0), m_uint64Value(value), m_doubleValue(0.0), 
    m_uuidValue(nullCuuid) {
  }
#else
  Param(const std::string &name, const long int value) :
    m_name(name), m_type(LOG_MSG_PARAM_INT),
    m_strValue(), m_intValue(value), m_uint64Value(0), m_doubleValue(0.0),
    m_uuidValue(nullCuuid) {
  }
#endif

  /**
   * Constructor for long unsigned int
   */
#if defined __x86_64__
  Param(const std::string &name, const long unsigned int value) :
    m_name(name), m_type(LOG_MSG_PARAM_INT64),
    m_strValue(), m_intValue(0), m_uint64Value(value), m_doubleValue(0.0), 
    m_uuidValue(nullCuuid) {
  }
#else
  Param(const std::string &name, const long unsigned int value) :
    m_name(name), m_type(LOG_MSG_PARAM_INT),
    m_strValue(), m_intValue(value), m_uint64Value(0), m_doubleValue(0.0),
    m_uuidValue(nullCuuid) {
  }
#endif

  /**
   * Constructor for int
   */
  Param(const std::string &name, const int value) :
    m_name(name), m_type(LOG_MSG_PARAM_INT),
    m_strValue(), m_intValue(value), m_uint64Value(0), m_doubleValue(0.0),
    m_uuidValue(nullCuuid) {
  }

  /**
   * Constructor for int
   */
  Param(const std::string &name, const unsigned int value) :
    m_name(name), m_type(LOG_MSG_PARAM_INT),
    m_strValue(), m_intValue(value), m_uint64Value(0), m_doubleValue(0.0),
    m_uuidValue(nullCuuid) {
  }

  /**
   * Constructor for u_signed64
   */
  Param(const std::string &name, const u_signed64 value) :
    m_name(name), m_type(LOG_MSG_PARAM_INT64),
    m_strValue(), m_intValue(0), m_uint64Value(value), m_doubleValue(0.0),
    m_uuidValue(nullCuuid) {
  }

  /**
   * Constructor for floats
   */
  Param(const std::string &name, const float value) :
    m_name(name), m_type(LOG_MSG_PARAM_DOUBLE),
    m_strValue(), m_intValue(0), m_uint64Value(0), m_doubleValue(value),
    m_uuidValue(nullCuuid) {
  }

  /**
   * Constructor for doubles
   */
  Param(const std::string &name, const double value) :
    m_name(name), m_type(LOG_MSG_PARAM_DOUBLE),
    m_strValue(), m_intValue(0), m_uint64Value(0), m_doubleValue(value),
    m_uuidValue(nullCuuid) {
  }

  /**
   * Constructor for Tape VIDS
   */
  Param(const std::string &name, castor::stager::TapeVid &value) :
    m_name(name), m_type(LOG_MSG_PARAM_TPVID),
    m_strValue(0 != value.vid() ? value.vid() : ""), m_intValue(0),
    m_uint64Value(0), m_doubleValue(0.0), m_uuidValue(nullCuuid) {
  }

  /**
   * Constructor for Raw parameters
   */
  Param(const std::string &rawParams) :
    m_name(""), m_type(LOG_MSG_PARAM_RAW),
    m_strValue(rawParams), m_intValue(0), m_uint64Value(0), m_doubleValue(0.0),
    m_uuidValue(nullCuuid) {
  };

  /**
   * Constructor for IPAddress
   */
  Param(const std::string &name, const castor::log::IPAddress &value);

  /**
   * Constructor for TimeStamp
   */
  Param(const std::string &name, const castor::log::TimeStamp &value);

  /**
   * Constructor for objects
   */
  Param(const std::string &name, const castor::IObject *const value);

  /**
   * Returns a const reference to the name of the parameter.
   */
  const std::string &getName() const {
    return m_name;
  }

  /**
   * Returns the type of the parameter.
   */
  int getType() const {
    return m_type;
  }

  /**
   * Returns a const refverence to the string value if there is one, else an
   * empty string.
   */
  const std::string &getStrValue() const {
    if(LOG_MSG_PARAM_STR == m_type) {
      return m_strValue;
    } else {
      return m_emptyStr;
    }
  }

  /**
   * Returns the int value if there is one, else 0.
   */
  int getIntValue() const {
    if(LOG_MSG_PARAM_INT == m_type) {
      return m_intValue;
    } else {
      return 0;
    }
  }

  /**
   * Returns the unsigned 64-bit int value if there is one, else 0.
   */
  u_signed64 getUint64Value() const {
    if(LOG_MSG_PARAM_INT64 == m_type) {
      return m_uint64Value;
    } else {
      return (u_signed64)0;
    }
  }

  /**
   * Returns the double value if there is one, else 0.0;
   */
  double getDoubleValue() const {
    if(LOG_MSG_PARAM_DOUBLE == m_type) {
      return m_doubleValue;
    } else {
      return (double)0.0;
    }
  }

  /**
   * Returns a const reference to the UUID value if there is one, else
   * nullCuuid.
   */
  const Cuuid_t &getUuidValue() const {
    if(LOG_MSG_PARAM_UUID == m_type) {
      return m_uuidValue;
    } else {
      return nullCuuid;
    }
  }

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
