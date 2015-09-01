/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "common/strerror_r_wrapper.hpp"
#include "common/Utils.hpp"

#include <attr/xattr.h>
#include <limits>
#include <memory>
#include <shift/serrno.h>
#include <sstream>
#include <stdlib.h>
#include <strings.h>
#include <sys/types.h>
#include <uuid/uuid.h>

using cta::exception::Exception;

//------------------------------------------------------------------------------
// assertPathStartsWithASlash
//------------------------------------------------------------------------------
static void assertPathStartsWithASlash(const std::string &path) {
  if(path.empty()) {
    throw Exception("Path is an empty string");
  }

  if('/' != path[0]) {
    throw Exception("Path does not start with a '/' character");
  }
}

//------------------------------------------------------------------------------
// isValidPathChar
//------------------------------------------------------------------------------
static bool isValidPathChar(const char c) {
  return ('0' <= c && c <= '9') ||
         ('A' <= c && c <= 'Z') ||
         ('a' <= c && c <= 'z') ||
         c == '_'               ||
         c == '/'               ||
         c == '.';
}

//------------------------------------------------------------------------------
// assertValidPathChar
//------------------------------------------------------------------------------
static void assertValidPathChar(const char c) {
  if(!isValidPathChar(c)) {
    std::ostringstream message;
    message << "The '" << c << "' character cannot be used within a path";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// assertPathContainsValidChars
//------------------------------------------------------------------------------
static void assertPathContainsValidChars(const std::string &path) {
  for(std::string::const_iterator itor = path.begin(); itor != path.end();
    itor++) {
    assertValidPathChar(*itor);
  }
}

//------------------------------------------------------------------------------
// assertPathDoesContainConsecutiveSlashes
//------------------------------------------------------------------------------
static void assertPathDoesContainConsecutiveSlashes(const std::string &path) {
  char previousChar = '\0';

  for(std::string::const_iterator itor = path.begin(); itor != path.end();
    itor++) {
    const char &currentChar  = *itor;
    if(previousChar == '/' && currentChar == '/') {
      throw Exception("Path contains consecutive slashes");
    }
    previousChar = currentChar;
  }
}

//------------------------------------------------------------------------------
// assertAbsolutePathSyntax
//------------------------------------------------------------------------------
void cta::Utils::assertAbsolutePathSyntax(const std::string &path) {
  try {
    assertPathStartsWithASlash(path);
    assertPathContainsValidChars(path);
    assertPathDoesContainConsecutiveSlashes(path);
  } catch(std::exception &ex) {
    std::ostringstream message;
    message << "Absolute path \"" << path << "\" contains a syntax error: " <<
      ex.what();
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// getEnclosingPath
//------------------------------------------------------------------------------
std::string cta::Utils::getEnclosingPath(const std::string &path) {
  if(path == "/") {
    throw Exception("Root directory does not have a parent");
  }

  const std::string::size_type lastSlashIndex = path.find_last_of('/');
  if(std::string::npos == lastSlashIndex) {
    throw Exception("Path does not contain a slash");
  }
  return path.substr(0, lastSlashIndex + 1);
}

//------------------------------------------------------------------------------
// getEnclosedName
//------------------------------------------------------------------------------
std::string cta::Utils::getEnclosedName(const std::string &path) {
  const std::string::size_type last_slash_idx = path.find_last_of('/');
  if(std::string::npos == last_slash_idx) {
    return path;
  } else {
    if(path.length() == 1) {
      return "";
    } else {
      return path.substr(last_slash_idx + 1);
    }
  }
}

//-----------------------------------------------------------------------------
// getEnclosedNames
//-----------------------------------------------------------------------------
std::list<std::string> cta::Utils::getEnclosedNames(
  const std::list<std::string> &paths) {
  std::list<std::string> names;

  for(std::list<std::string>::const_iterator itor = paths.begin();
    itor != paths.end(); itor++) {
    names.push_back(getEnclosedName(*itor));
  }

  return names;
}

//-----------------------------------------------------------------------------
// trimSlashes
//-----------------------------------------------------------------------------
std::string cta::Utils::trimSlashes(const std::string &s) {
  // Find first non slash character
  size_t beginpos = s.find_first_not_of("/");
  std::string::const_iterator it1;
  if (std::string::npos != beginpos) {
    it1 = beginpos + s.begin();
  } else {
    it1 = s.begin();
  }

  // Find last non slash chararacter
  std::string::const_iterator it2;
  size_t endpos = s.find_last_not_of("/");
  if (std::string::npos != endpos) {
    it2 = endpos + 1 + s.begin();
  } else {
    it2 = s.end();
  }
  
  return std::string(it1, it2);
}

//-----------------------------------------------------------------------------
// trimFinalSlashes
//-----------------------------------------------------------------------------
std::string cta::Utils::trimFinalSlashes(const std::string &s) {
  // Find last non slash chararacter
  std::string::const_iterator it2;
  size_t endpos = s.find_last_not_of("/");
  if (std::string::npos != endpos) {
    it2 = endpos + 1 + s.begin();
  } else {
    it2 = s.end();
  }
  
  return std::string(s.begin(), it2);
}

//-----------------------------------------------------------------------------
// splitString
//-----------------------------------------------------------------------------
void cta::Utils::splitString(const std::string &str, const char separator,
  std::vector<std::string> &result) {

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
// generateUuid
//-----------------------------------------------------------------------------
std::string cta::Utils::generateUuid() {
  uuid_t uuid;
  char str[36 + 1];

  uuid_generate(uuid);
  uuid_unparse_lower(uuid, str);

  return str;
}

//-----------------------------------------------------------------------------
// endsWith
//-----------------------------------------------------------------------------
bool cta::Utils::endsWith(const std::string &str, const char c) {
  if(str.empty()) {
    return false;
  } else {
    return c == str.at(str.length() - 1);
  }
}

//------------------------------------------------------------------------------
// setXattr
//------------------------------------------------------------------------------
void cta::Utils::setXattr(const std::string &path, const std::string &name,
  const std::string &value) {
  if(setxattr(path.c_str(), name.c_str(), value.c_str(), value.length(), 0)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << "Call to setxattr() failed: path=" << path << " name=" << name <<
      " value=" << value << ": " << Utils::errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// getXattr
//------------------------------------------------------------------------------
std::string cta::Utils::getXattr(const std::string &path,
  const std::string &name) {
  const auto sizeOfValue = getxattr(path.c_str(), name.c_str(), NULL, 0);
  if(0 > sizeOfValue) {
    const int savedErrno = errno;
    std::stringstream msg;
    msg << "Call to getxattr() failed: path=" << path << " name=" << name <<
      ": " << Utils::errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if(0 == sizeOfValue) {
    return "";
  }

  std::unique_ptr<char[]> value(new char[sizeOfValue + 1]);
  bzero(value.get(), sizeOfValue + 1);

  if(0 > getxattr(path.c_str(), name.c_str(), (void *)value.get(),
    sizeOfValue)) {
    const int savedErrno = errno;
    std::stringstream msg;      
    msg << "Call to getxattr() failed: path=" << path << " name=" << name << 
      ": " << Utils::errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  return value.get();
}

//------------------------------------------------------------------------------
// errnoToString
//------------------------------------------------------------------------------
std::string cta::Utils::errnoToString(const int errnoValue) throw() {
  char buf[100];

  if(!strerror_r_wrapper(errnoValue, buf, sizeof(buf))) {
    return buf;
  } else {
    const int errnoSetByStrerror_r_wrapper = errno;
    std::ostringstream oss;

    switch(errnoSetByStrerror_r_wrapper) {
    case EINVAL:
      oss << "Failed to convert errnoValue to string: Invalid errnoValue"
        ": errnoValue=" << errnoValue;
      break;
    case ERANGE:
      oss << "Failed to convert errnoValue to string"
        ": Destination buffer for error string is too small"
        ": errnoValue=" << errnoValue;
      break;
    default:
      oss << "Failed to convert errnoValue to string"
        ": strerror_r_wrapper failed in an unknown way"
        ": errnoValue=" << errnoValue;
      break;
    }

    return oss.str();
  }
}

//------------------------------------------------------------------------------
// serrnoToString
//------------------------------------------------------------------------------
std::string cta::Utils::serrnoToString(const int serrnoValue) throw() {
  char buf[100];
  if(!sstrerror_r(serrnoValue, buf, sizeof(buf))) {;
    return buf;
  } else {
    std::ostringstream oss;
    oss << "Failed to convert serrnoValue to string"
      ": sstrerror_r returned -1: serrnoValue=" << serrnoValue;
    return oss.str();
  }
}

//------------------------------------------------------------------------------
// toUint16
//------------------------------------------------------------------------------
uint16_t cta::Utils::toUint16(const std::string &str) {
  if(str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uint16_t: An empty string is not"
      " a valid unsigned integer";
    throw exception::Exception(msg.str());
  }

  errno = 0;
  const long int value = strtol(str.c_str(), (char **) NULL, 10);
  const int savedErrno = errno;
  if(savedErrno) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint16_t: " <<
      errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if(0 > value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint16_t: Negative number";
    throw exception::Exception(msg.str());
  }

  if(65535 < value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint16_t: Number too big";
    throw exception::Exception(msg.str());
  } 

  return value;
}

//------------------------------------------------------------------------------
// toUid
//------------------------------------------------------------------------------
uid_t cta::Utils::toUid(const std::string &str) {
  if(str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uid_t: An empty string is not"
      " a valid uid_t value";
    throw exception::Exception(msg.str());
  }

  errno = 0;
  const long int value = strtol(str.c_str(), (char **) NULL, 10);
  const int savedErrno = errno;
  if(savedErrno) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uid_t: " <<
      errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if(0 > value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uid_t: Negative number";
    throw exception::Exception(msg.str());
  }

  if(std::numeric_limits<uid_t>::max() < value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uid_t: Number too big";
    throw exception::Exception(msg.str());
  }

  return value;
}

//------------------------------------------------------------------------------
// toGid
//------------------------------------------------------------------------------
gid_t cta::Utils::toGid(const std::string &str) {
  if(str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to gid_t: An empty string is not"
      " a valid gid_t value";
    throw exception::Exception(msg.str());
  }
  
  errno = 0;
  const long int value = strtol(str.c_str(), (char **) NULL, 10);
  const int savedErrno = errno;
  if(savedErrno) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to gid_t: " <<
      errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if(0 > value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to gid_t: Negative number";
    throw exception::Exception(msg.str());
  }
  
  if(std::numeric_limits<gid_t>::max() < value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to gid_t: Number too big";
    throw exception::Exception(msg.str());
  }
  
  return value;
} 

//------------------------------------------------------------------------------
// isValidUInt
//------------------------------------------------------------------------------
bool cta::Utils::isValidUInt(const std::string &str)
  throw() {
  // An empty string is not a valid unsigned integer
  if(str.empty()) {
    return false;
  }

  // For each character in the string
  for(std::string::const_iterator itor = str.begin(); itor != str.end();
    itor++) {

    // If the current character is not a valid numerical digit
    if(*itor < '0' || *itor > '9') {
      return false;
    }
  }

  return true;
}
