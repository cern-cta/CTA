/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Exception.hpp"
#include "common/exception/Errnum.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"

#include <regex>
#include <algorithm>
#include <attr/xattr.h>
#include <limits>
#include <memory>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <uuid/uuid.h>
#include <zlib.h>
#include <sys/utsname.h>
#include <sys/prctl.h>
#include <iomanip>

using cta::exception::Exception;

//------------------------------------------------------------------------------
// assertPathStartsWithASlash
//------------------------------------------------------------------------------
static void assertPathStartsWithASlash(const std::string& path) {
  if (path.empty()) {
    throw Exception("Path is an empty string");
  }

  if ('/' != path[0]) {
    throw Exception("Path does not start with a '/' character");
  }
}

//------------------------------------------------------------------------------
// isValidPathChar
//------------------------------------------------------------------------------
static bool isValidPathChar(const char c) {
  return ('0' <= c && c <= '9') || ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || c == '_' || c == '/' || c == '.';
}

//------------------------------------------------------------------------------
// assertValidPathChar
//------------------------------------------------------------------------------
static void assertValidPathChar(const char c) {
  if (!isValidPathChar(c)) {
    std::ostringstream message;
    message << "The '" << c << "' character cannot be used within a path";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// assertPathContainsValidChars
//------------------------------------------------------------------------------
static void assertPathContainsValidChars(const std::string& path) {
  for (std::string::const_iterator itor = path.begin(); itor != path.end(); ++itor) {
    assertValidPathChar(*itor);
  }
}

//------------------------------------------------------------------------------
// assertPathDoesContainConsecutiveSlashes
//------------------------------------------------------------------------------
//static void assertPathDoesContainConsecutiveSlashes(const std::string &path) {
//  char previousChar = '\0';
//
//  for(std::string::const_iterator itor = path.begin(); itor != path.end();
//    itor++) {
//    const char &currentChar  = *itor;
//    if(previousChar == '/' && currentChar == '/') {
//      throw Exception("Path contains consecutive slashes");
//    }
//    previousChar = currentChar;
//  }
//}

namespace cta::utils {

//------------------------------------------------------------------------------
// isValidIPAddress
//------------------------------------------------------------------------------
bool isValidIPAddress(const std::string& address) {
  // clang-format off
   const Regex ipv4Regex("((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])");
   const Regex ipv6Regex("(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|"            // 1:2:3:4:5:6:7:8
                         "([0-9a-fA-F]{1,4}:){1,7}:|"                            // 1::                              1:2:3:4:5:6:7::
                         "([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|"            // 1::8             1:2:3:4:5:6::8  1:2:3:4:5:6::8
                         "([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|"     // 1::7:8           1:2:3:4:5::7:8  1:2:3:4:5::8
                         "([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|"     // 1::6:7:8         1:2:3:4::6:7:8  1:2:3:4::8
                         "([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|"     // 1::5:6:7:8       1:2:3::5:6:7:8  1:2:3::8
                         "([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|"     // 1::4:5:6:7:8     1:2::4:5:6:7:8  1:2::8
                         "[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|"          // 1::3:4:5:6:7:8   1::3:4:5:6:7:8  1::8
                         ":((:[0-9a-fA-F]{1,4}){1,7}|:)|"                        // ::2:3:4:5:6:7:8  ::2:3:4:5:6:7:8 ::8       ::
                         "fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|"        // fe80::7:8%eth0   fe80::7:8%1     (link-local IPv6 addresses with zone index)
                         "::(ffff(:0{1,4}){0,1}:){0,1}"
                         "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}"
                         "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|"             // ::255.255.255.255 ::ffff:255.255.255.255 ::ffff:0:255.255.255.255 (IPv4-mapped IPv6 addresses and IPv4-translated addresses)
                         "([0-9a-fA-F]{1,4}:){1,4}:"
                         "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}"
                         "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))");           // 2001:db8:3:4::192.0.2.33  64:ff9b::192.0.2.33 (IPv4-Embedded IPv6 Address)
  // clang-format on
  return ipv4Regex.has_match(address) || ipv6Regex.has_match(address);
}

//------------------------------------------------------------------------------
// assertIsFQDN
//------------------------------------------------------------------------------
void assertIsFQDN(const std::string& hostname) {
  const Regex hostnameRegex("^(([a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]\\.)|([a-zA-Z0-9]\\.))+[a-zA-Z]{2,63}$");

  if (hostname.length() > 253 || !hostnameRegex.has_match(hostname)) {
    throw Exception(hostname + " is not a valid Fully-Qualified Domain Name.");
  }
}

//------------------------------------------------------------------------------
// assertAbsolutePathSyntax
//------------------------------------------------------------------------------
void assertAbsolutePathSyntax(const std::string& path) {
  try {
    assertPathStartsWithASlash(path);
    assertPathContainsValidChars(path);
    //assertPathDoesContainConsecutiveSlashes(path);
  } catch (std::exception& ex) {
    std::ostringstream message;
    message << "Absolute path \"" << path << "\" contains a syntax error: " << ex.what();
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// getEnclosingPath
//------------------------------------------------------------------------------
std::string getEnclosingPath(const std::string& path) {
  if (path == "/") {
    throw Exception("Root directory does not have a parent");
  }
  const std::string& trimmedPath = trimFinalSlashes(path);

  const std::string::size_type lastSlashIndex = trimmedPath.find_last_of('/');
  if (std::string::npos == lastSlashIndex) {
    throw Exception("Path does not contain a slash");
  }
  return trimmedPath.substr(0, lastSlashIndex + 1);
}

//------------------------------------------------------------------------------
// getEnclosedName
//------------------------------------------------------------------------------
std::string getEnclosedName(const std::string& path) {
  const std::string::size_type last_slash_idx = path.find_last_of('/');
  if (std::string::npos == last_slash_idx) {
    return path;
  } else {
    if (path.length() == 1) {
      return "";
    } else {
      return path.substr(last_slash_idx + 1);
    }
  }
}

//-----------------------------------------------------------------------------
// getEnclosedNames
//-----------------------------------------------------------------------------
std::list<std::string> getEnclosedNames(const std::list<std::string>& paths) {
  std::list<std::string> names;

  for (std::list<std::string>::const_iterator itor = paths.begin(); itor != paths.end(); ++itor) {
    names.push_back(getEnclosedName(*itor));
  }

  return names;
}

//-----------------------------------------------------------------------------
// trimSlashes
//-----------------------------------------------------------------------------
std::string trimSlashes(const std::string& s) {
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
std::string trimFinalSlashes(const std::string& s) {
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
void splitString(const std::string& str, const char separator, std::vector<std::string>& result) {
  if (str.empty()) {
    return;
  }

  std::string::size_type beginIndex = 0;
  std::string::size_type endIndex = str.find(separator);

  while (endIndex != std::string::npos) {
    result.push_back(str.substr(beginIndex, endIndex - beginIndex));
    beginIndex = ++endIndex;
    endIndex = str.find(separator, endIndex);
  }

  result.push_back(str.substr(beginIndex));
}

//-----------------------------------------------------------------------------
// trimString
//-----------------------------------------------------------------------------
std::string trimString(std::string_view s) {
  const std::string& spaces = "\t\n\v\f\r ";

  // Find first non-space character
  size_t beginpos = s.find_first_not_of(spaces);
  std::string_view::const_iterator it1 = s.begin() + (beginpos == std::string_view::npos ? 0 : beginpos);

  // Find last non-space chararacter
  size_t endpos = s.find_last_not_of(spaces);
  // endpos+1 because the string constructor with iterator does not include the character pointed to by the end iterator
  std::string_view::const_iterator it2 = s.begin() + (endpos == std::string_view::npos ? 0 : endpos + 1);

  return std::string(it1, it2);
}

//------------------------------------------------------------------------------
// postEllipsis
//------------------------------------------------------------------------------
std::string postEllipsis(const std::string& s, size_t maxSize) {
  std::string ellipsis = "[...]";
  if (maxSize < ellipsis.size()) {
    throw cta::exception::Exception("In cta::utils::postEllipsis(): maxSize cannot be smaller than ellipsis size");
  }
  if (s.size() <= maxSize) {
    return s;
  }
  return s.substr(0, maxSize - ellipsis.size()) + ellipsis;
}

//------------------------------------------------------------------------------
// midEllipsis
//------------------------------------------------------------------------------
std::string midEllipsis(const std::string& s, size_t maxSize, size_t beginingSize) {
  std::string ellipsis = "[...]";
  if (maxSize < ellipsis.size() + beginingSize) {
    throw cta::exception::Exception(
      "In cta::utils::midEllipsis(): maxSize cannot be smaller than ellipsis size + beginingSize");
  }
  if (s.size() <= maxSize) {
    return s;
  }
  if (!beginingSize) {
    beginingSize = (maxSize - ellipsis.size()) / 2;
  }
  return s.substr(0, beginingSize) + ellipsis + s.substr(s.size() - maxSize + ellipsis.size() + beginingSize);
}

//------------------------------------------------------------------------------
// preEllipsis
//------------------------------------------------------------------------------
std::string preEllipsis(const std::string& s, size_t maxSize) {
  std::string ellipsis = "[...]";
  if (maxSize < ellipsis.size()) {
    throw cta::exception::Exception("In cta::utils::postEllipsis(): maxSize cannot be smaller than ellipsis size");
  }
  if (s.size() <= maxSize) {
    return s;
  }
  return ellipsis + s.substr(s.size() - maxSize + ellipsis.size());
}

//------------------------------------------------------------------------------
// singleSpaceString
//------------------------------------------------------------------------------
std::string singleSpaceString(const std::string& str) {
  bool inWhitespace = false;
  bool strContainsNonWhiteSpace = false;

  // Output string stream used to construct the result
  std::ostringstream result;

  // For each character in the original string
  for (std::string::const_iterator itor = str.begin(); itor != str.end(); ++itor) {
    // If the character is a space or a tab
    if (*itor == ' ' || *itor == '\t') {
      // Remember we are in whitespace
      inWhitespace = true;

      // Else the character is not a space or a tab
    } else {
      // If we are leaving whitespace
      if (inWhitespace) {
        // Remember we have left whitespace
        inWhitespace = false;

        // Remember str contains non-whitespace
        strContainsNonWhiteSpace = true;

        // Insert a single space into the output string stream
        result << " ";
      }

      // Insert the character into the output string stream
      result << *itor;
    }
  }

  // If str is not emtpy and does not contain any non-whitespace characters
  // then nothing has been written to the result stream, therefore write a
  // single space
  if (!str.empty() && !strContainsNonWhiteSpace) {
    result << " ";
  }

  return result.str();
}

//-----------------------------------------------------------------------------
// generateUuid
//-----------------------------------------------------------------------------
std::string generateUuid() {
  uuid_t uuid;
  char str[36 + 1];

  uuid_generate(uuid);
  uuid_unparse_lower(uuid, str);

  return str;
}

//-----------------------------------------------------------------------------
// endsWith
//-----------------------------------------------------------------------------
bool endsWith(const std::string& str, const char c) {
  if (str.empty()) {
    return false;
  } else {
    return c == str.at(str.length() - 1);
  }
}

//------------------------------------------------------------------------------
// setXattr
//------------------------------------------------------------------------------
void setXattr(const std::string& path, const std::string& name, const std::string& value) {
  if (setxattr(path.c_str(), name.c_str(), value.c_str(), value.length(), 0)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << "Call to setxattr() failed: path=" << path << " name=" << name << " value=" << value << ": "
        << errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// getXattr
//------------------------------------------------------------------------------
std::string getXattr(const std::string& path, const std::string& name) {
  const auto sizeOfValue = getxattr(path.c_str(), name.c_str(), nullptr, 0);
  if (0 > sizeOfValue) {
    const int savedErrno = errno;
    std::stringstream msg;
    msg << "Call to getxattr() failed: path=" << path << " name=" << name << ": " << errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if (0 == sizeOfValue) {
    return "";
  }

  std::unique_ptr<char[]> value(new char[sizeOfValue + 1]);
  bzero(value.get(), sizeOfValue + 1);

  if (0 > getxattr(path.c_str(), name.c_str(), (void*) value.get(), sizeOfValue)) {
    const int savedErrno = errno;
    std::stringstream msg;
    msg << "Call to getxattr() failed: path=" << path << " name=" << name << ": " << errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  return value.get();
}

//------------------------------------------------------------------------------
// getAdler32
//------------------------------------------------------------------------------
uint32_t getAdler32(const uint8_t* buf, const uint32_t len) {
  const uint32_t checksum = adler32(0L, Z_NULL, 0);
  return adler32(checksum, (const Bytef*) buf, len);
}

//------------------------------------------------------------------------------
// getShortHostname
//------------------------------------------------------------------------------
std::string getShortHostname() {
  struct utsname un;
  exception::Errnum::throwOnMinusOne(uname(&un));
  std::vector<std::string> snn;
  splitString(un.nodename, '.', snn);
  return snn.at(0);
}

//------------------------------------------------------------------------------
// getDumpableProcessAttribute
//------------------------------------------------------------------------------
bool getDumpableProcessAttribute() {
  const int rc = prctl(PR_GET_DUMPABLE);
  switch (rc) {
    case -1: {
      const std::string errStr = errnoToString(errno);
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to get the dumpable attribute of the process: " << errStr;
      throw ex;
    }
    case 0:
      return false;
    case 1:
      return true;
    case 2:
      return true;
    default: {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to get the dumpable attribute of the process"
                         ": Unknown value returned by prctl(): rc="
                      << rc;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// setDumpableProcessAttribute
//------------------------------------------------------------------------------
void setDumpableProcessAttribute(const bool dumpable) {
  const int rc = prctl(PR_SET_DUMPABLE, dumpable ? 1 : 0);
  switch (rc) {
    case -1: {
      const std::string errStr = errnoToString(errno);
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to set the dumpable attribute of the process: " << errStr;
      throw ex;
    }
    case 0:
      return;
    default: {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to set the dumpable attribute of the process"
                         ": Unknown value returned by prctl(): rc="
                      << rc;
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// hexDump
//-----------------------------------------------------------------------------
std::string hexDump(const void* mem, unsigned int n) {
  std::ostringstream out;
  const unsigned char* p = reinterpret_cast<const unsigned char*>(mem);
  for (unsigned int i = 0; i < n; i++) {
    if (0 != i) {
      out << " ";
    }
    out << std::uppercase << std::hex << std::setw(2) << std::setfill(out.widen('0')) << int(p[i]);
  }
  return out.str();
}

//-----------------------------------------------------------------------------
// copyString
//-----------------------------------------------------------------------------
void copyString(char* const dst, const size_t dstSize, const std::string& src) {
  if (dst == nullptr) {
    cta::exception::Exception ex;

    ex.getMessage() << __FUNCTION__ << ": Pointer to destination string is nullptr";

    throw ex;
  }

  if (src.length() >= dstSize) {
    cta::exception::Exception ex;

    ex.getMessage() << __FUNCTION__ << ": Source string is longer than destination.  Source length: " << src.length()
                    << " Max destination length: " << (dstSize - 1);

    throw ex;
  }

  strncpy(dst, src.c_str(), dstSize);
  *(dst + dstSize - 1) = '\0';  // Ensure destination string is null terminated
}

//-----------------------------------------------------------------------------
// getCurrentLocalTime
//-----------------------------------------------------------------------------
std::string getCurrentLocalTime() {
  ::timeval tv;
  ::gettimeofday(&tv, nullptr);
  ::time_t now = (::time_t) tv.tv_sec;
  ::time(&now);
  struct tm localNowBuf;
  struct tm* localNow = ::localtime_r(&now, &localNowBuf);
  char buff[80];
  char buff2[10];
  ::strftime(buff, sizeof(buff), "%b %e %H:%M:%S.", localNow);
  ::snprintf(buff2, sizeof(buff2), "%06ld", tv.tv_usec);
  return std::string(buff) + std::string(buff2);
}

//-----------------------------------------------------------------------------
// getCurrentLocalTime
//-----------------------------------------------------------------------------
std::string getCurrentLocalTime(const std::string& format) {
  ::timeval tv;
  ::gettimeofday(&tv, nullptr);
  ::time_t now = (::time_t) tv.tv_sec;
  ::time(&now);
  struct tm localNowBuf;
  struct tm* localNow = ::localtime_r(&now, &localNowBuf);
  char buff[80];
  ::strftime(buff, sizeof(buff), format.c_str(), localNow);
  return std::string(buff);
}

//------------------------------------------------------------------------------
// searchAndReplace
//------------------------------------------------------------------------------
int searchAndReplace(std::string& str, const std::string& search, const std::string& replacement) {
  std::string::size_type pos = 0;
  int num_replacements = 0;
  while (std::string::npos != (pos = str.find(search, pos))) {
    str.replace(pos, search.length(), replacement);
    pos += replacement.length();
    ++num_replacements;
  }
  return num_replacements;
}

//------------------------------------------------------------------------------
// segfault
//------------------------------------------------------------------------------
void segfault() {
  *((int*) nullptr) = 0;
}

//------------------------------------------------------------------------------
// appendParameterXRootFileURL
//------------------------------------------------------------------------------
void appendParameterXRootFileURL(std::string& fileURL, const std::string& parameterName, const std::string& value) {
  cta::utils::Regex regexXrootFile("^(root://.*)$");
  if (regexXrootFile.exec(fileURL).size()) {
    std::string parameterToAppend = parameterName + "=" + value;
    if (fileURL.find("?") == std::string::npos) {
      //No parameter at the end of the XRootd fileURL
      fileURL.append("?" + parameterToAppend);
      return;
    }
    //There are parameters in the fileURL, check if the parameter is
    //there or not
    if (fileURL.find("&" + parameterName) == std::string::npos) {
      fileURL.append("&" + parameterToAppend);
    }
  }
}

std::string removePrefix(const std::string& input, char prefixChar) {
  size_t position = input.find_first_not_of(prefixChar);
  if (position == std::string::npos) {
    return input;
  } else {
    return input.substr(position, input.size());
  }
}

std::string getEnv(const std::string& variableName) {
  const char* envVarC = std::getenv(variableName.c_str());
  if (envVarC == nullptr) {
    return "";
  }
  return std::string(envVarC);
}

std::list<std::string> commaSeparatedStringToList(const std::string& commaSeparated) {
  // Parse the elements between commas, while trimming any whitespaces before and after each string
  std::regex pattern_between_commas(R"rgx(\s*([^,]*[^\s,])\s*,?)rgx");
  auto it_begin = std::sregex_iterator(commaSeparated.begin(), commaSeparated.end(), pattern_between_commas);
  auto it_end = std::sregex_iterator();

  std::list<std::string> result;
  for (std::sregex_iterator it = it_begin; it != it_end; ++it) {
    std::smatch match = *it;
    result.emplace_back(match[1]);
  }
  return result;
}

std::string listToCommaSeparatedString(const std::list<std::string>& list) {
  std::ostringstream oss;
  bool is_first_value = true;
  for (const auto& value : list) {
    if (!is_first_value) {
      oss << ",";
    }
    oss << value;
    is_first_value = false;
  }

  return oss.str();
}

std::string decimalToHexadecimal(const std::string& decimalNumberStr) {
  std::stringstream fxidStream;
  const auto decimalNumber = stol(decimalNumberStr);
  fxidStream << std::hex << decimalNumber;
  return fxidStream.str();
}

std::string hexadecimalToDecimal(const std::string& hexadecimalNumberStr) {
  std::stringstream fidStream;
  const auto decimalNumber = stol(hexadecimalNumberStr, nullptr, 16);
  fidStream << std::dec << decimalNumber;
  return fidStream.str();
}

// Check if Hexadecimal is correct
bool isValidHex(const std::string& hex) {
  if (hex.empty()) {
    return false;
  }

  size_t start = 0;
  if (hex.length() > 2 && hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) {
    start = 2;
  }

  for (size_t i = start; i < hex.length(); i++) {
    if (!std::isxdigit(hex[i])) {
      return false;
    }
  }

  return true;
}

std::time_t getCurrentEpochTime() {
  return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

}  // namespace cta::utils
