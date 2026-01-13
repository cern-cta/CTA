/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SourcedParameter.hpp"

#include "common/utils/StringConversions.hpp"

#include <algorithm>

namespace cta {

template<>
void SourcedParameter<time_t>::set(const std::string& value, const std::string& source) {
  if (!utils::isValidUInt(value)) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<time_t>::set() : badly formatted integer"
                    << " for category=" << m_category << " key=" << m_key << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(value) >> m_value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<uint16_t>::set(const std::string& value, const std::string& source) {
  if (!utils::isValidUInt(value)) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<uint16_t>::set() : badly formatted integer"
                    << " for category=" << m_category << " key=" << m_key << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(value) >> m_value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<uint32_t>::set(const std::string& value, const std::string& source) {
  if (!utils::isValidUInt(value)) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<uint32_t>::set() : badly formatted integer"
                    << " for category=" << m_category << " key=" << m_key << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(value) >> m_value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<uint64_t>::set(const std::string& value, const std::string& source) {
  if (!utils::isValidUInt(value)) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<uint64_t>::set() : badly formatted integer"
                    << " for category=" << m_category << " key=" << m_key << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(value) >> m_value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<bool>::set(const std::string& value, const std::string& source) {
  std::string lower = value;
  cta::utils::toLower(lower);

  if (lower == "true" || lower == "1" || lower == "yes") {
    m_value = true;
  } else if (lower == "false" || lower == "0" || lower == "no") {
    m_value = false;
  } else {
    BadlyFormattedBoolean ex;
    ex.getMessage() << "In SourcedParameter<bool>::set() : badly formatted boolean"
                    << " for category=" << m_category << " key=" << m_key << " value='" << value << "' at:" << source;
    throw ex;
  }

  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<std::string>::set(const std::string& value, const std::string& source) {
  m_value = value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<time_t>::addLogParamForValue(log::LogContext& lc) {
  lc.push({"value", m_value});
}

template<>
void SourcedParameter<uint16_t>::addLogParamForValue(log::LogContext& lc) {
  lc.push({"value", m_value});
}

template<>
void SourcedParameter<uint32_t>::addLogParamForValue(log::LogContext& lc) {
  lc.push({"value", m_value});
}

template<>
void SourcedParameter<uint64_t>::addLogParamForValue(log::LogContext& lc) {
  lc.push({"value", m_value});
}

template<>
void SourcedParameter<std::string>::addLogParamForValue(log::LogContext& lc) {
  lc.push({"value", m_value});
}

template<>
void SourcedParameter<bool>::addLogParamForValue(log::LogContext& lc) {
  lc.push({"value", m_value});
}

}  // namespace cta
