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
 * @(#)Param.cpp,v 1.2 $Release$ 2005/04/05 13:36:36 sponcec3
 *
 * A parameter for the DLF (Distributed Logging System)
 *
 * @author castor dev team
 *****************************************************************************/

// Include Files
#include <time.h>
#include <sstream>
#include <iomanip>
#include "castor/dlf/Param.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/IObject.hpp"
#include <string.h>

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::dlf::Param::Param(const char* name,
                          castor::IObject* value) :
  m_deallocate(true) {
  m_cParam.name = (char*) name;
  m_cParam.type = DLF_MSG_PARAM_STR;
  std::ostringstream s;
  castor::ObjectSet set;
  value->print(s, "", set);
  m_cParam.value.par_string = strdup(s.str().c_str());
}

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::dlf::Param::Param(const char* name,
                          castor::dlf::IPAddress value) :
  m_deallocate(true) {
  m_cParam.name = (char*) name;
  m_cParam.type = DLF_MSG_PARAM_STR;
  std::ostringstream s;
  s << ((value.ip() & 0xFF000000) >> 24) << "."
    << ((value.ip() & 0x00FF0000) >> 16) << "."
    << ((value.ip() & 0x0000FF00) >> 8) << "."
    << ((value.ip() & 0x000000FF));
  m_cParam.value.par_string = strdup(s.str().c_str());
}

//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::dlf::Param::Param(const char* name,
                          castor::dlf::TimeStamp value) :
  m_deallocate(true) {
  m_cParam.name = (char*) name;
  m_cParam.type = DLF_MSG_PARAM_STR;
  time_t time = value.time();
  // There is NO localtime_r() on Windows,
  // so we will use non-reentrant version localtime().
  struct tm tmstruc;
#if !defined(_WIN32)
  localtime_r (&time, &tmstruc);
#else
  struct tm* p_tmstruc;
  p_tmstruc = localtime(&time);
  tmstruc = *p_tmstruc;
#endif
  std::ostringstream s;
  s << std::setw(2) << tmstruc.tm_mon+1
    << "/" << tmstruc.tm_mday
    << " " << tmstruc.tm_hour
    << ":" << tmstruc.tm_min
    << ":" << tmstruc.tm_sec;
  m_cParam.value.par_string = strdup(s.str().c_str());
}
