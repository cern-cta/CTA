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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>

namespace cta::catalogue {

class SchemaVersion {
public:
  class Builder;

  using MajorMinor = std::pair<uint64_t,uint64_t>;

  enum class Status{
    UPGRADING,
    PRODUCTION
  };

  SchemaVersion() = default;

  virtual ~SchemaVersion() = default;

  template<typename T>
  T getSchemaVersion() const;
  template<typename T>
  T getSchemaVersionNext() const;
  template<typename T>
  T getStatus() const;

private:
  uint64_t m_schemaVersionMajor = 0;
  uint64_t m_schemaVersionMinor = 0;
  SchemaVersion::Status m_status = Status::UPGRADING;
  std::optional<uint64_t> m_nextSchemaVersionMajor;
  std::optional<uint64_t> m_nextSchemaVersionMinor;
};

class SchemaVersion::Builder{
public:
  Builder() = default;
  Builder& schemaVersionMajor(const uint64_t schemaVersionMajor);
  Builder& schemaVersionMinor(const uint64_t schemaVersionMinor);
  Builder& nextSchemaVersionMajor(const uint64_t pNextSchemaVersionMajor);
  Builder& nextSchemaVersionMinor(const uint64_t pNextSchemaVersionMinor);
  Builder& status(const std::string& status);
  Builder& status(const SchemaVersion::Status & pstatus);
  SchemaVersion build() const;
private:
  void validate() const;
  SchemaVersion m_schemaVersion;

  bool m_schemaVersionMajorSet = false;
  bool m_schemaVersionMinorSet = false;
  static std::map<std::string, SchemaVersion::Status, std::less<>> s_mapStringStatus;
};

} // namespace cta::catalogue
