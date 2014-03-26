/******************************************************************************
 *                      castor/utils/utils.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/utils/utils.hpp"

#include <algorithm>
#include <errno.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <locale>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

//-----------------------------------------------------------------------------
// writeTime
//-----------------------------------------------------------------------------
void castor::utils::writeTime(std::ostream &os, const time_t time,
  const char* const format) {

  tm localTime;

  localtime_r(&time, &localTime);

  const std::time_put<char>& dateWriter =
    std::use_facet<std::time_put<char> >(os.getloc());
  const size_t n = strlen(format);

  if (dateWriter.put(os, os, ' ', &localTime, format, format + n).failed()){
    os << "UNKNOWN";
  }
}

//-----------------------------------------------------------------------------
// splitString
//-----------------------------------------------------------------------------
void castor::utils::splitString(const std::string &str,
  const char separator, std::vector<std::string> &result) throw() {

  if(str.empty()) {
    return;
  }

  std::string::size_type beginIndex = 0;
  std::string::size_type endIndex   = str.find(separator);

  while(endIndex != std::string::npos) {
    result.push_back(str.substr(beginIndex, endIndex - beginIndex));
    beginIndex = ++endIndex;
    endIndex = str.find(separator, endIndex);
  }

  // If no separator could not be found then simply append the whole input
  // string to the result
  if(endIndex == std::string::npos) {
    result.push_back(str.substr(beginIndex, str.length()));
  }
}

//------------------------------------------------------------------------------
// isValidUInt
//------------------------------------------------------------------------------
bool castor::utils::isValidUInt(const char *str)
  throw() {
  // An empty string is not a valid unsigned integer
  if(*str == '\0') {
    return false;
  }

  // For each character in the string
  for(;*str != '\0'; str++) {
    // If the current character is not a valid numerical digit
    if(*str < '0' || *str > '9') {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// toUpper
//------------------------------------------------------------------------------
void castor::utils::toUpper(char *str) {
  for(;*str != '\0'; str++) {
    *str = toupper(*str);
  }
}

//------------------------------------------------------------------------------
// toUpper
//------------------------------------------------------------------------------
void castor::utils::toUpper(std::string &str) {
  for(std::string::iterator itor=str.begin(); itor!=str.end(); itor++) {
    *itor = toupper(*itor);
  }
}

//---------------------------------------------------------------------------
// getTimeOfDay
//---------------------------------------------------------------------------
void castor::utils::getTimeOfDay(struct timeval *const tv,
  struct timezone *const tz) throw(castor::exception::Exception) {
  if(0 != gettimeofday(tv, tz)) {
    const int savedErrno = errno;
    char errBuf[100];
    castor::exception::Internal ex;
    ex.getMessage() << "Call to gettimeofday() failed: " <<
      sstrerror_r(savedErrno, errBuf, sizeof(errBuf));
    throw ex;
  }
}

//---------------------------------------------------------------------------
// timevalGreaterThan
//---------------------------------------------------------------------------
bool castor::utils::timevalGreaterThan(const timeval &a, const timeval &b)
  throw() {
  if(a.tv_sec != b.tv_sec) {
    return a.tv_sec > b.tv_sec;
  } else {
    return a.tv_usec > b.tv_usec;
  }
}

//---------------------------------------------------------------------------
// timevalAbsDiff
//---------------------------------------------------------------------------
timeval castor::utils::timevalAbsDiff(const timeval &a, const timeval &b)
  throw() {
  timeval bigger  = {0, 0};
  timeval smaller = {0, 0};
  timeval result  = {0, 0};

  // If time-values a and b are equal
  if(a.tv_sec == b.tv_sec && a.tv_usec == b.tv_usec) {
    return result; // Result was initialised to {0, 0}
  }

  // The time-values are not equal, determine which is the bigger and which is
  // the smaller time-value
  if(timevalGreaterThan(a, b)) {
    bigger  = a;
    smaller = b;
  } else {
    bigger  = b;
    smaller = a;
  }

  // Subtract the smaller time-value from the bigger time-value carrying over
  // 1000000 micro-seconds from the seconds to the micro-seconds if necessay
  if(bigger.tv_usec >= smaller.tv_usec) {
    result.tv_usec = bigger.tv_usec - smaller.tv_usec;
    result.tv_sec  = bigger.tv_sec  - smaller.tv_sec;
  } else {
    result.tv_usec = bigger.tv_usec + 1000000 - smaller.tv_usec;
    result.tv_sec  = bigger.tv_sec - 1 - smaller.tv_sec;
  }

  return result;
}

//---------------------------------------------------------------------------
// timevalToDouble
//---------------------------------------------------------------------------
double castor::utils::timevalToDouble(const timeval &tv) throw() {
  return tv.tv_sec + tv.tv_usec / 1000000.0;
}

//-----------------------------------------------------------------------------
// copyString
//-----------------------------------------------------------------------------
void castor::utils::copyString(char *const dst, const size_t dstSize,
  const char *const src) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __FUNCTION__
      << ": Pointer to destination string is NULL";

    throw ex;
  }

  if(src == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __FUNCTION__
      << ": Pointer to source string is NULL";

    throw ex;
  }

  const size_t srcLen = strlen(src);

  if(srcLen >= dstSize) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __FUNCTION__
      << ": Source string is longer than destination.  Source length: "
      << srcLen << " Max destination length: " << (dstSize - 1);

    throw ex;
  }

  strncpy(dst, src, dstSize);
  *(dst + dstSize -1) = '\0'; // Ensure destination string is null terminated
}
