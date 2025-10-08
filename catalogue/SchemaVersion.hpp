/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
