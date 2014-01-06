/******************************************************************************
 *                      Param.cpp
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
 * A parameter for the CASTOR logging system
 *
 * @author castor dev team
 *****************************************************************************/

// Include Files
#include <time.h>
#include <sstream>
#include <string.h>
#include <iomanip>

#include "castor/log/Constants.hpp"
#include "castor/log/Param.hpp"
#include "castor/ObjectSet.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name, const std::string &value)
  throw():
  m_name(name),
  m_type(name == "TPVID" ? LOG_MSG_PARAM_TPVID : LOG_MSG_PARAM_STR),
  m_strValue(value), m_intValue(0), m_uint64Value(0), m_doubleValue(0.0),
  m_uuidValue(nullCuuid) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name, const Cuuid_t value) throw():
  m_name(name), m_type(LOG_MSG_PARAM_UUID),
  m_strValue(), m_intValue(0), m_uint64Value(0), m_doubleValue(0.0),
  m_uuidValue(value) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const Cuuid_t &value) throw() :
  m_name(""), m_type(LOG_MSG_PARAM_UUID),
  m_strValue(), m_intValue(0), m_uint64Value(0), m_doubleValue(0.0), 
  m_uuidValue(value) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
#if defined __x86_64__
castor::log::Param::Param(const std::string &name, const long int value)
  throw():
  m_name(name), m_type(LOG_MSG_PARAM_INT64),
  m_strValue(), m_intValue(0), m_uint64Value(value), m_doubleValue(0.0), 
  m_uuidValue(nullCuuid) {
}
#else
castor::log::Param::Param(const std::string &name, const long int value)
  throw():
  m_name(name), m_type(LOG_MSG_PARAM_INT),
  m_strValue(), m_intValue(value), m_uint64Value(0), m_doubleValue(0.0),
  m_uuidValue(nullCuuid) {
}
#endif

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
#if defined __x86_64__
castor::log::Param::Param(const std::string &name,
  const long unsigned int value) throw():
  m_name(name), m_type(LOG_MSG_PARAM_INT64),
  m_strValue(), m_intValue(0), m_uint64Value(value), m_doubleValue(0.0), 
  m_uuidValue(nullCuuid) {
}
#else
castor::log::Param::Param(const std::string &name,
  const long unsigned int value) throw():
  m_name(name), m_type(LOG_MSG_PARAM_INT),
  m_strValue(), m_intValue(value), m_uint64Value(0), m_doubleValue(0.0),
  m_uuidValue(nullCuuid) {
}
#endif

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name, const int value) throw():
  m_name(name), m_type(LOG_MSG_PARAM_INT),
  m_strValue(), m_intValue(value), m_uint64Value(0), m_doubleValue(0.0),
  m_uuidValue(nullCuuid) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name, const unsigned int value)
  throw():
  m_name(name), m_type(LOG_MSG_PARAM_INT),
  m_strValue(), m_intValue(value), m_uint64Value(0), m_doubleValue(0.0),
  m_uuidValue(nullCuuid) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name, const u_signed64 value)
  throw():
  m_name(name), m_type(LOG_MSG_PARAM_INT64),
  m_strValue(), m_intValue(0), m_uint64Value(value), m_doubleValue(0.0),
  m_uuidValue(nullCuuid) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name, const float value)
  throw():
  m_name(name), m_type(LOG_MSG_PARAM_DOUBLE),
  m_strValue(), m_intValue(0), m_uint64Value(0), m_doubleValue(value),
  m_uuidValue(nullCuuid) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name, const double value)
  throw():
  m_name(name), m_type(LOG_MSG_PARAM_DOUBLE),
  m_strValue(), m_intValue(0), m_uint64Value(0), m_doubleValue(value),
  m_uuidValue(nullCuuid) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::log::Param::Param(const std::string &rawParams) throw():
  m_name(""), m_type(LOG_MSG_PARAM_RAW),
  m_strValue(rawParams), m_intValue(0), m_uint64Value(0), m_doubleValue(0.0),
  m_uuidValue(nullCuuid) {
}

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name,
  const castor::IObject *const value) throw():
  m_name(name), m_type(LOG_MSG_PARAM_STR),
  m_intValue(0), m_uint64Value(0), m_doubleValue(0.0), m_uuidValue(nullCuuid) {
  std::ostringstream oss;
  castor::ObjectSet set;
  value->print(oss, "", set);
  m_strValue = oss.str();
}

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name,
  const castor::log::IPAddress &value) throw():
  m_name(name), m_type(LOG_MSG_PARAM_STR),
  m_intValue(0), m_uint64Value(0), m_doubleValue(0.0), m_uuidValue(nullCuuid) {
  std::ostringstream oss;
  oss << ((value.ip() & 0xFF000000) >> 24) << "."
      << ((value.ip() & 0x00FF0000) >> 16) << "."
      << ((value.ip() & 0x0000FF00) >> 8) << "."
      << ((value.ip() & 0x000000FF));
  m_strValue = oss.str();
}

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name,
  const castor::log::TimeStamp &value) throw():
  m_name(name), m_type(LOG_MSG_PARAM_STR),
  m_intValue(0), m_uint64Value(0), m_doubleValue(0.0), m_uuidValue(nullCuuid) {
  time_t time = value.time();
  // There is NO localtime_r() on Windows,
  // so we will use non-reentrant version localtime().
  struct tm tmstruc;
  localtime_r (&time, &tmstruc);
  std::ostringstream oss;
  oss << std::setw(2) << tmstruc.tm_mon+1
      << "/" << tmstruc.tm_mday
      << " " << tmstruc.tm_hour
      << ":" << tmstruc.tm_min
      << ":" << tmstruc.tm_sec;
  m_strValue = oss.str();
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &castor::log::Param::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getType
//------------------------------------------------------------------------------
int castor::log::Param::getType() const throw() {
  return m_type;
}

//------------------------------------------------------------------------------
// getStrValue
//------------------------------------------------------------------------------
const std::string &castor::log::Param::getStrValue() const throw() {
  if(LOG_MSG_PARAM_STR == m_type || LOG_MSG_PARAM_RAW == m_type) {
    return m_strValue;
  } else {
    return m_emptyStr;
  }
}

//------------------------------------------------------------------------------
// getIntValue
//------------------------------------------------------------------------------
int castor::log::Param::getIntValue() const throw() {
  if(LOG_MSG_PARAM_INT == m_type) {
    return m_intValue;
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// getUint64Value
//------------------------------------------------------------------------------
u_signed64 castor::log::Param::getUint64Value() const throw() {
  if(LOG_MSG_PARAM_INT64 == m_type) {
    return m_uint64Value;
  } else {
    return (u_signed64)0;
  }
}

//------------------------------------------------------------------------------
// getDoubleValue
//------------------------------------------------------------------------------
double castor::log::Param::getDoubleValue() const throw() {
  if(LOG_MSG_PARAM_DOUBLE == m_type) {
    return m_doubleValue;
  } else {
    return (double)0.0;
  }
}

//------------------------------------------------------------------------------
// getUuidValue
//------------------------------------------------------------------------------
const Cuuid_t &castor::log::Param::getUuidValue() const throw() {
  if(LOG_MSG_PARAM_UUID == m_type) {
    return m_uuidValue;
  } else {
    return nullCuuid;
  }
}
