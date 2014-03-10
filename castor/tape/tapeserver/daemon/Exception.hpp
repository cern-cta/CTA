/******************************************************************************
 *                      Exception.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <exception>
#include <string>
#include <errno.h>
#include <string.h>
#include <stdio.h>

class MemException: public std::exception {
public:
  MemException(const std::string & what): m_what(what) {}
  virtual ~MemException() throw() {}
  virtual const char* what() const throw() {
    return m_what.c_str();
  } 
private:
  std::string m_what;
};

void errnoExeptLauncher(int ret, void * obj) {
  if(ret) {
    char s[1000];
    char t[1000];
#ifdef _GNU_SOURCE
    char * errorStr = ::strerror_r(ret, s, sizeof(s));
    if (!errorStr) {
      snprintf(t, sizeof(t), "Failed to get error string for ret=%d obj=0x%p", ret, obj);
      throw MemException(t);
    } else {
      snprintf(t, sizeof(t), "%s (ret=%d) obj=0x%p",errorStr, ret, obj);
      throw MemException(t);
    }
#else /* This should work for CLANG on MAC */
    int r2 = ::strerror_r(ret, s, sizeof(s));
    if (!r2) {
      snprintf(t, sizeof(t), "Failed to get error string for ret=%d obj=0x%p", ret, obj);
      throw MemException(t);
    } else {
      snprintf(t, sizeof(t), "%s (ret=%d) obj=0x%p",s, ret, obj);
      throw MemException(t);
    }
#endif
  }
}
