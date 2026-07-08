/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "mountdecision/schema/MountDecisionSchema.hpp"
#include "mountdecision/schema/SchemaUtils.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

#include <iostream>
#include <string>

namespace {

void printUsage(std::ostream& os) {
  os << "Usage:" << std::endl
     << "    cta-mount-decision-schema-create databaseConnectionFile [options]" << std::endl
     << "Where:" << std::endl
     << "    databaseConnectionFile" << std::endl
     << "        The path to the file containing the connection details of the CTA" << std::endl
     << "        Mount Decision DB" << std::endl
     << "Options:" << std::endl
     << "    -h,--help" << std::endl
     << "        Prints this usage message" << std::endl;
}

}  // namespace

int main(const int argc, char* const* const argv) {
  try {
    if (argc == 2 && (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help")) {
      printUsage(std::cout);
      return 0;
    }
    if (argc != 2) {
      printUsage(std::cerr);
      return 1;
    }

    const auto login = cta::rdbms::Login::parseFile(argv[1]);
    if (login.dbType != cta::rdbms::Login::DBTYPE_POSTGRESQL) {
      throw cta::exception::Exception("Mount Decision DB schema creation only supports PostgreSQL.");
    }

    cta::rdbms::ConnPool connPool(login, 1);
    auto conn = connPool.getConn();
    if (cta::mountdecision::tableExists("MOUNT_DECISION_KV", conn)) {
      std::cerr << "Cannot create the database schema because the MOUNT_DECISION_KV table already exists" << std::endl;
      return 1;
    }
    cta::mountdecision::executeNonQueries(conn, cta::mountdecision::MountDecisionSchema::sql);
    conn.commit();
    return 0;
  } catch (cta::exception::Exception& ex) {
    std::cerr << "Aborting: " << ex.getMessage().str() << std::endl;
  } catch (std::exception& ex) {
    std::cerr << "Aborting: " << ex.what() << std::endl;
  }
  return 1;
}
