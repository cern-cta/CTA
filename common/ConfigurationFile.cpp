/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ConfigurationFile.hpp"
#include "common/exception/Exception.hpp"

#include <fstream>
#include <algorithm>

namespace cta {

ConfigurationFile::ConfigurationFile(const std::string& path) {
  // Try to open the configuration file, throwing an exception if there is a
  // failure
  std::ifstream file(path);
  if (file.fail()) {
    cta::exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": Failed to open configuration file"
      ": m_fileName=" << path;
    throw ex;
  }

  std::string line;
  size_t lineNumber=0;
  while(++lineNumber, std::getline(file, line)) {
    // get rid of potential tabs
    std::replace(line.begin(),line.end(),'\t',' ');
    // get the category
    std::istringstream sline(line);
    std::string category;
    if (!(sline >> category)) continue; // empty line
    if (category[0] == '#') continue;   // comment
    // get the key
    std::string key;
    if (!(sline >> key)) continue;      // no key on line
    if (key[0] == '#') continue;        // key commented
    // get and store value
    while (sline.get() == ' '){}; sline.unget(); // skip spaces
    std::string value;
    std::getline(sline, value, '#');
    value.erase(value.find_last_not_of(" \n\r\t")+1); // right trim
    auto & entry = entries[category][key];
    entry.value = value;
    entry.line = lineNumber;
  }
}

} // namespace cta
