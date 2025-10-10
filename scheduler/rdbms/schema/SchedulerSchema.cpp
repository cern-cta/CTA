/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/schema/SchedulerSchema.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::schedulerdb {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SchedulerSchema::SchedulerSchema(): sql("") {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SchedulerSchema::SchedulerSchema(const std::string &sqlSchema): sql(sqlSchema) {
}

//------------------------------------------------------------------------------
// getSchemaVersion
//------------------------------------------------------------------------------
std::map<std::string, uint64_t, std::less<>> SchedulerSchema::getSchemaVersion() const {
  try {
    std::map<std::string, uint64_t, std::less<>> schemaVersion;
    cta::utils::Regex schemaVersionRegex(
      "INSERT INTO CTA_SCHEDULER\\("
      "  SCHEMA_VERSION_MAJOR,"
      "  SCHEMA_VERSION_MINOR\\)"
      "VALUES\\("
      "  ([[:digit:]]+),"
      "  ([[:digit:]]+)\\);"
    );
    auto version = schemaVersionRegex.exec(sql);
    if (3 == version.size()) {
      schemaVersion.insert(std::make_pair("SCHEMA_VERSION_MAJOR", cta::utils::toUint64(version[1].c_str())));
      schemaVersion.insert(std::make_pair("SCHEMA_VERSION_MINOR", cta::utils::toUint64(version[2].c_str())));
    } else {
      exception::Exception ex;
      ex.getMessage() << "Could not find SCHEMA_VERSION";
      throw ex;
    }
    return schemaVersion;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::schedulerdb
