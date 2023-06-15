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

#include "SourcedParameter.hpp"

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
void SourcedParameter<std::string>::set(const std::string& value, const std::string& source) {
  m_value = value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<time_t>::addLogParamForValue(log::LogContext& lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<uint16_t>::addLogParamForValue(log::LogContext& lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<uint32_t>::addLogParamForValue(log::LogContext& lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<uint64_t>::addLogParamForValue(log::LogContext& lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<std::string>::addLogParamForValue(log::LogContext& lc) {
  lc.pushOrReplace({"value", m_value});
}

}  // namespace cta
