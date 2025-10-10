/**
 * SPDX-FileCopyrightText: 2019 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <limits>
#include <algorithm>

#include "common/exception/UserError.hpp"
#include "Config.hpp"

namespace cta::common {

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
  } catch (std::exception& ex) {
    throw exception::UserError("Failed to parse configuration file " + filename + ": " + ex.what());
  }
}

const optionlist_t& Config::getOptionList(const std::string& key) const {
  auto it = m_configuration.find(key);
  return it == m_configuration.end() ? m_nulloptionlist : it->second;
}

std::vector<std::string> Config::getOptionValueStrVector(const std::string& key) const {
  return getOptionList(key);
}

std::optional<std::string> Config::getOptionValueStr(const std::string& key) const {
  auto optionlist = getOptionList(key);

  return optionlist.empty() ? std::nullopt : std::optional<std::string>(optionlist.at(0));
}

std::optional<int> Config::getOptionValueInt(const std::string& key) const {
  auto optionlist = getOptionList(key);

  return optionlist.empty() ? std::nullopt : std::optional<int>(std::stoi(optionlist.at(0)));
}

std::optional<uint32_t> Config::getOptionValueUInt(const std::string& key) const {
  auto optionlist = getOptionList(key);

  return optionlist.empty() ? std::nullopt : std::optional<uint32_t>(stou<uint32_t>(optionlist.at(0)));
}

std::optional<bool> Config::getOptionValueBool(const std::string& key) const {
  auto optionlist = getOptionList(key);

  if (optionlist.empty()) {
    return std::nullopt;
  }

  std::string strval = optionlist.at(0);
  std::transform(strval.begin(), strval.end(), strval.begin(), ::tolower);

  if (strval == "true") {
    return std::optional<bool>(true);
  } else if (strval == "false") {
    return std::optional<bool>(false);
  } else {
    throw std::invalid_argument("\"" + optionlist.at(0) + "\" cannot be converted to Boolean type");
  }
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

template<class T>
T Config::stou(const std::string& strVal) const {
  try {
    std::size_t uiPos = 0;
    auto lVal = stoll(strVal, &uiPos);
    // Do not accept value like: 123abc
    if (uiPos != strVal.size()) {
      throw std::invalid_argument("Invalid argument");
    }
    if (lVal < 0) {
      throw std::out_of_range("Negative value");
    }
    if (static_cast<uint64_t>(lVal) > std::numeric_limits<T>::max()) {
      throw std::out_of_range("Above maximum value");
    }
  } catch (const std::invalid_argument& ex) {
    std::ostringstream strErrMsg;
    strErrMsg << "stou(" << strVal << "): Invalid argument";
    throw std::invalid_argument(strErrMsg.str());
  } catch (const std::out_of_range& ex) {
    std::ostringstream strErrMsg;
    strErrMsg << "stou(" << strVal << "): " << ex.what();
    if (std::numeric_limits<int64_t>::max() < std::numeric_limits<T>::max()) {
      strErrMsg << ", expected correct value is in the range [0, " << std::numeric_limits<int64_t>::max() << "]";
    } else {
      strErrMsg << ", expected correct value is in the range [0, " << std::numeric_limits<T>::max() << "]";
    }
    throw std::out_of_range(strErrMsg.str());
  }

  auto ulVal = std::stoul(strVal);

  return static_cast<T>(ulVal);
}

}  // namespace cta::common
