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

#include "SourcedParameter.hpp"
#include "tapeserver/daemon/FetchReportOrFlushLimits.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"
#include <algorithm>


namespace cta {
namespace tape {
namespace daemon {

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
void SourcedParameter<FetchReportOrFlushLimits>::set(const std::string & value, 
  const std::string & source) {
  // We expect an entry in the form "<size limit>, <file limit>"
  // There should be one and only one comma in the parameter.
  if (1 != std::count(value.begin(), value.end(), ',')) {
    BadlyFormattedSizeFileLimit ex;
    ex.getMessage() << "In SourcedParameter<FetchReportOrFlushLimits>::set() : badly formatted entry: one (and only one) comma expected"
        << " for category=" << m_category << " key=" << m_key 
        << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  // We can now split the entry
  std::string bytes, files;
  size_t commaPos=value.find(',');
  bytes=value.substr(0, commaPos);
  files=value.substr(commaPos+1);
  bytes=utils::trimString(bytes);
  files=utils::trimString(files);
  if (!(utils::isValidUInt(bytes)&&utils::isValidUInt(files))) {
    BadlyFormattedInteger ex;
    ex.getMessage() << "In SourcedParameter<FetchReportOrFlushLimits>::set() : badly formatted integer"
        << " for category=" << m_category << " key=" << m_key 
        << " value=\'" << value << "' at:" << source;
    throw ex;
  }
  std::istringstream(bytes) >> m_value.maxBytes;
  std::istringstream(files) >> m_value.maxFiles;
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
void SourcedParameter<uint64_t>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<std::string>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"value", m_value});
}

template<>
void SourcedParameter<FetchReportOrFlushLimits>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"maxBytes", m_value.maxBytes});
  lc.pushOrReplace({"maxFiles", m_value.maxFiles});
}

template<>
void SourcedParameter<TpconfigLine>::addLogParamForValue(log::LogContext & lc) {
  lc.pushOrReplace({"category", "TPCONFIG Entry"});
  lc.pushOrReplace({"unitName", m_value.unitName});
  lc.pushOrReplace({"logicalLibrary", m_value.logicalLibrary});
  lc.pushOrReplace({"devFilename", m_value.devFilename});
  lc.pushOrReplace({"librarySlot", m_value.rawLibrarySlot});
}

}}} // namespace cta::tape::daemon
