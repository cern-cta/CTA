/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "scheduler/PostgresSchedDB/schema/SchedulerSchema.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace postgresscheddb {

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
std::map<std::string, uint64_t> SchedulerSchema::getSchemaVersion() const {
  try {
    std::map<std::string, uint64_t> schemaVersion;
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

} // namespace postgresscheddb
} // namespace cta
