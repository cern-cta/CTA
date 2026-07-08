/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "mountdecision/schema/SchemaUtils.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

#include <iostream>
#include <string>

namespace {

void printUsage(std::ostream& os) {
  os << "Usage:" << std::endl
     << "    cta-mount-decision-schema-drop databaseConnectionFile [options]" << std::endl
     << "Where:" << std::endl
     << "    databaseConnectionFile" << std::endl
     << "        The path to the file containing the connection details of the CTA" << std::endl
     << "        Mount Decision DB" << std::endl
     << "Options:" << std::endl
     << "    -h,--help" << std::endl
     << "        Prints this usage message" << std::endl;
}

bool userConfirmsDropOfSchema(std::istream& in, std::ostream& out, const cta::rdbms::Login& login) {
  out << "WARNING" << std::endl;
  out << "You are about to drop the Mount Decision DB schema from the following database:" << std::endl;
  out << "    Database: " << login.connectionString << std::endl;
  out << "Are you sure you want to continue?" << std::endl;

  std::string userResponse;
  while (userResponse != "yes" && userResponse != "no") {
    out << R"#(Please type either "yes" or "no" > )#";
    std::getline(in, userResponse);
  }
  return userResponse == "yes";
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
      throw cta::exception::Exception("Mount Decision DB schema drop only supports PostgreSQL.");
    }

    cta::rdbms::ConnPool connPool(login, 1);
    auto conn = connPool.getConn();
    if (!cta::mountdecision::tableExists("MOUNT_DECISION_KV", conn)) {
      std::cout << "MOUNT_DECISION_KV table does not exist. Assuming the schema has already been dropped." << std::endl;
      return 0;
    }

    if (userConfirmsDropOfSchema(std::cin, std::cout, login)) {
      conn.executeNonQuery("DROP TABLE IF EXISTS MOUNT_DECISION_KV CASCADE");
      conn.commit();
      std::cout << "Dropped table MOUNT_DECISION_KV" << std::endl;
    } else {
      std::cout << "Aborting" << std::endl;
    }
    return 0;
  } catch (cta::exception::Exception& ex) {
    std::cerr << "Aborting: " << ex.getMessage().str() << std::endl;
  } catch (std::exception& ex) {
    std::cerr << "Aborting: " << ex.what() << std::endl;
  }
  return 1;
}
