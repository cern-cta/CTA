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

#include "Configuration.hpp"

#include <fstream>
#include <stdexcept>
#include <exception>
	

namespace cta {
namespace cmdline {

//------------------------------------------------------------------------------
// s_fileFormat
//------------------------------------------------------------------------------
const char *Configuration::s_fileFormat = "<FQDN>:<port>";

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Configuration::Configuration(
  const std::string &filename):
  filename(filename) {
  frontendHostAndPort = parseFile(filename);
}

//------------------------------------------------------------------------------
// parseFile
//------------------------------------------------------------------------------
std::string Configuration::parseFile(const std::string &filename) {
  try {
    std::ifstream file(filename);
    if (!file) {
      throw std::runtime_error("Failed to open ");
    }
    return parseStream(file);
  } catch(std::exception &ex) {
    throw std::runtime_error(std::string("Failed to parse configuration file " +
                                         filename + ": " + ex.what()));
  }
}

//------------------------------------------------------------------------------
// parseFile
//------------------------------------------------------------------------------
std::string Configuration::getFrontendHostAndPort() throw() {
  return frontendHostAndPort;
}

//------------------------------------------------------------------------------
// parseStream
//------------------------------------------------------------------------------
std::string Configuration::parseStream(std::istream &inputStream) {
  const std::list<std::string> lines = readNonEmptyLines(inputStream);

  if(1 != lines.size()) {
    throw std::runtime_error("There should only be one and only one line "
                             "containing a frontend hostname and port");
  }

  const std::string frontendHostAndPort = lines.front();
  std::vector<std::string> configurationLine;
  splitString(frontendHostAndPort, ' ', configurationLine);
  if(1 != configurationLine.size()) {
    throw std::runtime_error(std::string("There should not be any space in the "
                                         "host configuration: Correct"
                                         " format is ") + s_fileFormat);
  }
  std::vector<std::string> components;
  splitString(frontendHostAndPort, ':', components);
  if(2 != components.size()) {
    throw std::runtime_error(std::string("Port not found in the configuration:"
                                         " Correct format is ") + s_fileFormat);
  }
  const std::string &hostname = components[0];
  const std::string &port = components[1];
  if(!onlyContainsNumerals(port)) {
    throw std::runtime_error(std::string("Port must only contain numerals: "
                                         "value=") + port);
  }
  if(4 > hostname.length()) {
    throw std::runtime_error(std::string("Hostname too short: "
                                         "value=") + hostname);
  }
  return frontendHostAndPort;
}
//-----------------------------------------------------------------------------
// splitString
//-----------------------------------------------------------------------------
void Configuration::splitString(const std::string &str, const char separator,
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

//------------------------------------------------------------------------------
// onlyContainsNumerals
//------------------------------------------------------------------------------
bool Configuration::onlyContainsNumerals(const std::string &str) throw() {
  for(std::string::const_iterator itor = str.begin(); itor != str.end();
    itor++) {
    if(*itor < '0' || *itor  > '9') {
      return false;
    }
  }
  return true;
}

//-----------------------------------------------------------------------------
// trimString
//-----------------------------------------------------------------------------
std::string Configuration::trimString(const std::string &s) throw() {
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
// readNonEmptyLines
//------------------------------------------------------------------------------
std::list<std::string> Configuration::readNonEmptyLines(std::istream &inputStream) {

  std::list<std::string> lines;
  std::string line;

  while(std::getline(inputStream, line)) {
    // Remove the newline character if there is one
    {
      const std::string::size_type newlinePos = line.find("\n");
      if(newlinePos != std::string::npos) {
        line = line.substr(0, newlinePos);
      }
    }

    // If there is a comment, then remove it from the line
    {
      const std::string::size_type startOfComment = line.find("#");
      if(startOfComment != std::string::npos) {
        line = line.substr(0, startOfComment);
      }
    }

    // Left and right trim the line of whitespace
    line = Configuration::trimString(std::string(line));

    // If the line is not empty
    if(!line.empty()) {
      lines.push_back(line);
    }
  }

  return lines;
}
} // namesapce cmdline
} // namespace cta
