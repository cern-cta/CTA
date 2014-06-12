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
#include <sys/prctl.h>
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

//-----------------------------------------------------------------------------
// trimString
//-----------------------------------------------------------------------------
std::string castor::utils::trimString(const std::string &s) throw() {
  const std::string& spaces="\t\n\v\f\r ";

  // Find first non white character
  size_t beginpos = s.find_first_not_of(spaces);
  std::string::const_iterator it1;
  if (std::string::npos != beginpos) {
    it1 = beginpos + s.begin();
  } else {
    it1 = s.begin();
  }

  // Find last non white chararacter
  std::string::const_iterator it2;
  size_t endpos = s.find_last_not_of(spaces);
  if (std::string::npos != endpos) {
    it2 = endpos + 1 + s.begin();
  } else {
    it2 = s.end();
  }

  return std::string(it1, it2);
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
void castor::utils::getTimeOfDay(struct timeval *const tv)  {
  if(0 != gettimeofday(tv, NULL)) {
    const int savedErrno = errno;
    char errBuf[100];
    castor::exception::Exception ex;
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
  const char *const src) {

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

//------------------------------------------------------------------------------
// Tape DGNs and VIDs have the same rules about what characters they may
// contain.  This static and therefore hidden function implements this
// commonality.
//
// This function throws an InvalidArgument exception if the specified identifier
// string is syntactically incorrect.
//  
// The indentifier string is valid if each character is either a number (0-9),
// a letter (a-z, A-Z) or an underscore.
//    
// @param idTypeName The type name of the identifier, usually "DGN" or "VID".
// @param id The indentifier string to be checked.
// @param maxSize The maximum length the identifier string is permitted to have.
//------------------------------------------------------------------------------
static void checkDgnVidSyntax(const char *const idTypeName, const char *id,
  const size_t maxLen) {

  // Check the length of the identifier string
  const size_t len   = strlen(id);
  if(len > maxLen) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << idTypeName << " exceeds maximum length: actual=" << len
      << " max=" << maxLen;
    throw ex;
  }

  // Check each character of the identifier string
  char         c     = '\0';
  bool         valid = false;
  for(size_t i=0; i<len; i++) {
    c = id[i];
    valid = (c >= '0' && c <='9') || (c >= 'a' && c <= 'z') ||
      (c >= 'A' && c <= 'Z') || c == '_';

    if(!valid) {
      castor::exception::InvalidArgument ex;
      ex.getMessage() << idTypeName << " contains the invalid character '" << c
        << "'";
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// checkDgnSyntax
//------------------------------------------------------------------------------
void castor::utils::checkDgnSyntax(const char *dgn) {
  checkDgnVidSyntax("DGN", dgn, CA_MAXDGNLEN);
}

//------------------------------------------------------------------------------
// checkVidSyntax
//------------------------------------------------------------------------------
void castor::utils::checkVidSyntax(const char *vid) {
  checkDgnVidSyntax("VID", vid, CA_MAXVIDLEN);
}

//------------------------------------------------------------------------------
// getDumpableProcessAttribute
//------------------------------------------------------------------------------
bool castor::utils::getDumpableProcessAttribute() {
  const int rc = prctl(PR_GET_DUMPABLE);
  switch(rc) {
  case -1:
    {
      char errStr[100];
      sstrerror_r(errno, errStr, sizeof(errStr));
      errStr[sizeof(errStr) - 1] = '\0';
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to get the dumpable attribute of the process: " << errStr;
      throw ex;
    }
  case 0: return false;
  case 1: return true;
  case 2: return true;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to get the dumpable attribute of the process"
        ": Unknown value returned by prctl(): rc=" << rc;
      throw ex;
    }
  }
}


/**
 * Sets the attributes of the current process to indicate hat it will produce a
 * core dump if it receives a signal whose behaviour is to produce a core dump.
 *
 * @param dumpable true if the current program should be dumpable.
 */
void castor::utils::setDumpableProcessAttribute(const bool dumpable) {
  const int rc = prctl(PR_SET_DUMPABLE, dumpable ? 1 : 0);
  switch(rc) {
  case -1:
    {
      char errStr[100];
      sstrerror_r(errno, errStr, sizeof(errStr));
      errStr[sizeof(errStr) - 1] = '\0';
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to set the dumpable attribute of the process: " << errStr;
      throw ex;
    }
  case 0: return;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Failed to set the dumpable attribute of the process"
        ": Unknown value returned by prctl(): rc=" << rc;
      throw ex;
    }
  }
}
