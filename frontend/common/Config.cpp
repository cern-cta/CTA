/**
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2019-2022 CERN
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

#include <fstream>
#include <sstream>
#include <vector>
#include <map>

#include "common/exception/UserError.hpp"
#include "Config.hpp"

namespace cta {
namespace frontend {

//! Configuration option list type
using optionlist_t = std::vector<std::string>;

Config::Config(const std::string& filename) {
  // Open the config file for reading
  std::ifstream file(filename);
  if (!file) {
    throw exception::UserError("Failed to open " + filename);
  }
  // Parse the config file
  try {
    parse(file);
  }
  catch (std::exception& ex) {
    throw exception::UserError("Failed to parse configuration file " + filename + ": " + ex.what());
  }
}

const optionlist_t& Config::getOptionList(const std::string& key) const {
  auto it = m_configuration.find(key);
  return it == m_configuration.end() ? m_nulloptionlist : it->second;
}

std::optional<std::string> Config::getOptionValueStr(const std::string& key) const {
  auto optionlist = getOptionList(key);

  return optionlist.empty() ? std::nullopt : std::optional<std::string>(optionlist.at(0));
}

std::optional<int> Config::getOptionValueInt(const std::string& key) const {
  auto optionlist = getOptionList(key);

  return optionlist.empty() ? std::nullopt : std::optional<int>(std::stoi(optionlist.at(0)));
}

void Config::parse(std::ifstream& file) {
  std::string line;

  while (std::getline(file, line)) {
    // Strip out comments
    auto pos = line.find('#');
    if (pos != std::string::npos) {
      line.resize(pos);
    }

    // Extract the key
    std::istringstream ss(line);
    std::string key;
    ss >> key;

    // Extract and store the config options
    if (!key.empty()) {
      optionlist_t values = tokenize(ss);

      if (!values.empty()) {
        m_configuration[key] = values;
      }
    }
  }
}

optionlist_t Config::tokenize(std::istringstream& input) {
  optionlist_t values;

  while (!input.eof()) {
    std::string value;
    input >> value;
    if (!value.empty()) {
      values.push_back(value);
    }
  }

  return values;
}

}  // namespace frontend
}  // namespace cta
