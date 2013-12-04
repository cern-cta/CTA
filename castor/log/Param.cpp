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
#include <iomanip>
#include "castor/log/Param.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/IObject.hpp"
#include <string.h>

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::log::Param::Param(const std::string &name,
  const castor::IObject *const value) :
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
  const castor::log::IPAddress &value) :
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
  const castor::log::TimeStamp &value) :
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
