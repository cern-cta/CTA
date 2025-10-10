/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SchemaVersion.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

template<>
std::string SchemaVersion::getSchemaVersion() const {
  return std::to_string(m_schemaVersionMajor)+"."+std::to_string(m_schemaVersionMinor);
}

template<>
SchemaVersion::MajorMinor SchemaVersion::getSchemaVersion() const {
  return std::make_pair(m_schemaVersionMajor,m_schemaVersionMinor);
}

template<>
std::string SchemaVersion::getSchemaVersionNext() const {
  std::string schemaVersionNext;
  if(m_nextSchemaVersionMajor && m_nextSchemaVersionMinor){
    schemaVersionNext = std::to_string(m_nextSchemaVersionMajor.value())+"."+std::to_string(m_nextSchemaVersionMinor.value());
  }
  return schemaVersionNext;
}

template<>
SchemaVersion::MajorMinor SchemaVersion::getSchemaVersionNext() const {
  return std::make_pair(m_nextSchemaVersionMajor.value(),m_nextSchemaVersionMinor.value());
}

template<>
std::string SchemaVersion::getStatus() const {
  switch(m_status){
    case Status::PRODUCTION:
      return "PRODUCTION";
    case Status::UPGRADING:
      return "UPGRADING";
    default:
      throw cta::exception::Exception("In SchemaVersion::getStatus(), wrong status");
  }
}

template<>
SchemaVersion::Status SchemaVersion::getStatus() const {
  return m_status;
}

SchemaVersion::Builder& SchemaVersion::Builder::schemaVersionMajor(const uint64_t pSchemaVersionMajor){
  m_schemaVersion.m_schemaVersionMajor = pSchemaVersionMajor;
  m_schemaVersionMajorSet = true;
  return *this;
}

SchemaVersion::Builder& SchemaVersion::Builder::schemaVersionMinor(const uint64_t pSchemaVersionMinor){
  m_schemaVersion.m_schemaVersionMinor = pSchemaVersionMinor;
  m_schemaVersionMinorSet = true;
  return *this;
}

SchemaVersion::Builder& SchemaVersion::Builder::nextSchemaVersionMajor(const uint64_t pSchemaVersionMajorNext){
  m_schemaVersion.m_nextSchemaVersionMajor = pSchemaVersionMajorNext;
  return *this;
}

SchemaVersion::Builder& SchemaVersion::Builder::nextSchemaVersionMinor(const uint64_t pSchemaVersionMinorNext){
  m_schemaVersion.m_nextSchemaVersionMinor = pSchemaVersionMinorNext;
  return *this;
}

SchemaVersion::Builder& SchemaVersion::Builder::status(const std::string& pstatus){
  try {
    m_schemaVersion.m_status = s_mapStringStatus.at(pstatus);
    return *this;
  } catch(const std::out_of_range& ){
    throw cta::exception::Exception("In SchemaVersion::Builder::status(), wrong status given as parameter: "+pstatus);
  }
}

SchemaVersion::Builder& SchemaVersion::Builder::status(const SchemaVersion::Status & pstatus){
    m_schemaVersion.m_status = pstatus;
    return *this;
}

std::map<std::string,SchemaVersion::Status,std::less<>> SchemaVersion::Builder::s_mapStringStatus {
  {"PRODUCTION",Status::PRODUCTION},
  {"UPGRADING",Status::UPGRADING}
};

void SchemaVersion::Builder::validate() const {
  if(!m_schemaVersionMajorSet || !m_schemaVersionMinorSet){
    throw cta::exception::Exception("In SchemaVersion::Builder::validate(), schemaVersionMajor or schemaVersionMinor have not been set.");
  }
  if(m_schemaVersion.m_nextSchemaVersionMajor && m_schemaVersion.m_nextSchemaVersionMinor && m_schemaVersion.m_status == SchemaVersion::Status::PRODUCTION){
    throw cta::exception::Exception("In SchemaVersion::Builder::validate(), status is "+m_schemaVersion.getStatus<std::string>()+" but nextSchemaVersionMajor and nextSchemaVersionMinor are defined");
  }
  if(!m_schemaVersion.m_nextSchemaVersionMajor && !m_schemaVersion.m_nextSchemaVersionMinor && m_schemaVersion.m_status == SchemaVersion::Status::UPGRADING){
    throw cta::exception::Exception("In SchemaVersion::Builder::validate(), status is "+m_schemaVersion.getStatus<std::string>()+" but nextSchemaVersionMajor and nextSchemaVersionMinor are NOT defined.");
  }
}

SchemaVersion SchemaVersion::Builder::build() const {
  validate();
  return m_schemaVersion;
}

} // namespace cta::catalogue
