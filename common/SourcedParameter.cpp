/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "SourcedParameter.hpp"

#include <algorithm>


namespace cta {

template<>
void SourcedParameter<time_t>::set(const std::string & value, const std::string & source) {     
  if (!utils::isValidUInt(value)) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<time_t>::set() : badly formatted integer"
        << " for category=" << m_category << " key=" << m_key 
        << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(value) >> m_value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<uint32_t>:: set(const std::string &value, const std::string & source){
  if (!utils::isValidUInt(value)) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<uint32_t>::set() : badly formatted integer"
        << " for category=" << m_category << " key=" << m_key 
        << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(value) >> m_value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<uint64_t>::set(const std::string & value, const std::string & source) {     
  if (!utils::isValidUInt(value)) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<uint64_t>::set() : badly formatted integer"
        << " for category=" << m_category << " key=" << m_key 
        << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(value) >> m_value;
  m_source = source;
  m_set = true;
}

template<>  
void SourcedParameter<std::string>::set(const std::string & value, const std::string & source) {
  m_value = value;
  m_source = source;
  m_set = true;
}

template<>
void SourcedParameter<time_t>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<uint32_t>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<uint64_t>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<std::string>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"value", m_value});
}

} // namespace cta
