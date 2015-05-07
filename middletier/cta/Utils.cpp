/**
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

#include "cta/Exception.hpp"
#include "cta/Utils.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// checkAbsolutePathSyntax
//------------------------------------------------------------------------------
void cta::Utils::checkAbsolutePathSyntax(const std::string &path) {
  try {
    checkPathStartsWithASlash(path);
    checkPathContainsValidChars(path);
    checkPathDoesContainConsecutiveSlashes(path);
  } catch(std::exception &ex) {
    std::ostringstream message;
    message << "Absolute path \"" << path << "\" contains a syntax error: " <<
      ex.what();
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// checkPathStartsWithASlash
//------------------------------------------------------------------------------
void cta::Utils::checkPathStartsWithASlash(const std::string &path) {
  if(path.empty()) {
    throw Exception("Path is an empty string");
  }

  if('/' != path[0]) {
    throw Exception("Path does not start with a '/' character");
  }
}

//------------------------------------------------------------------------------
// checkPathContainsValidChars
//------------------------------------------------------------------------------
void cta::Utils::checkPathContainsValidChars(const std::string &path) {
  for(std::string::const_iterator itor = path.begin(); itor != path.end();
    itor++) {
    checkValidPathChar(*itor);
  }
}

//------------------------------------------------------------------------------
// checkValidPathChar
//------------------------------------------------------------------------------
void cta::Utils::checkValidPathChar(const char c) {
  if(!isValidPathChar(c)) {
    std::ostringstream message;
    message << "The '" << c << "' character cannot be used within a path";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// isValidPathChar
//------------------------------------------------------------------------------
bool cta::Utils::isValidPathChar(const char c) {
  return ('0' <= c && c <= '9') ||
         ('A' <= c && c <= 'Z') ||
         ('a' <= c && c <= 'z') ||
         c == '_'               ||
         c == '/';
}

//------------------------------------------------------------------------------
// checkPathDoesContainConsecutiveSlashes
//------------------------------------------------------------------------------
void cta::Utils::checkPathDoesContainConsecutiveSlashes(
  const std::string &path) {
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
// getEnclosingDirPath
//------------------------------------------------------------------------------
std::string cta::Utils::getEnclosingDirPath(const std::string &path) {
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
