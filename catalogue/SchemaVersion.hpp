/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#pragma once

#include <string>
#include <memory>
#include <map>
#include "common/optional.hpp"

namespace cta { namespace catalogue {

class SchemaVersion {
public:
  class Builder;
  
  typedef std::pair<uint64_t,uint64_t> MajorMinor;
  
  enum Status{
    UPGRADING,
    PRODUCTION
  };
  
  SchemaVersion(const SchemaVersion& orig);
  virtual ~SchemaVersion();
  
  template<typename T>
  T getSchemaVersion() const;
  template<typename T>
  T getSchemaVersionNext() const;
  template<typename T>
  T getStatus() const;
    
  SchemaVersion & operator=(const SchemaVersion &other);
private:
  uint64_t m_schemaVersionMajor;
  uint64_t m_schemaVersionMinor;
  cta::optional<uint64_t> m_nextSchemaVersionMajor;
  cta::optional<uint64_t> m_nextSchemaVersionMinor;
  SchemaVersion::Status m_status;
  SchemaVersion();
};

class SchemaVersion::Builder{
public:
  Builder();
  Builder(const Builder& );
  Builder & operator=(const Builder& builder);
  Builder & schemaVersionMajor(const uint64_t schemaVersionMajor);
  Builder & schemaVersionMinor(const uint64_t schemaVersionMinor);
  Builder & nextSchemaVersionMajor(const uint64_t pNextSchemaVersionMajor);
  Builder & nextSchemaVersionMinor(const uint64_t pNextSchemaVersionMinor);
  Builder & status(const std::string& status);
  Builder & status(const SchemaVersion::Status & pstatus);
  SchemaVersion build() const;
private:
  void validate() const;
  SchemaVersion m_schemaVersion;
  
  bool m_schemaVersionMajorSet = false;
  bool m_schemaVersionMinorSet = false;
  static std::map<std::string, SchemaVersion::Status> s_mapStringStatus;
};

}}
