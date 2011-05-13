/******************************************************************************
 *                test/castor/db/ora/OraTest.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef TEST_CASTOR_DB_ORA_ORATEST_HPP
#define TEST_CASTOR_DB_ORA_ORATEST_HPP 1

#include "castor/db/ora/SmartOcciResultSet.hpp"
#include "castor/tape/utils/utils.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <list>
#include <occi.h>
#include <stdlib.h>
#include <string>
#include <vector>

class OraTest: public CppUnit::TestFixture {
private:

  std::string getStagerDbConfigParam(std::list<std::string> configLines,
    const char *const service,  const char *const name)
    throw(castor::exception::Exception) {
    using namespace castor::tape;

    for(std::list<std::string>::const_iterator lineItor = configLines.begin();
      lineItor != configLines.end(); lineItor++) {

      std::vector<std::string> columns;
      utils::splitString(utils::singleSpaceString(utils::trimString(*lineItor)),
        ' ', columns);

      if(columns.size() >=3 && columns[0] == service && columns[1] == name) {
        return columns[2];
      }
    }

    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      "Failed to find stager configuration parameter"
      ": name=" << name;
    throw(ex);
  }

  void closeInSmartOcciResultSetDestructor(
    oracle::occi::Connection *const conn) {

    oracle::occi::Statement *stmt = conn->createStatement(
      "SELECT Dummy FROM Dual");

    {
      castor::db::ora::SmartOcciResultSet rs(stmt, stmt->executeQuery());

      while(rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
        std::string dummy = rs->getString(1);

        CPPUNIT_ASSERT_MESSAGE("Checking Dummy from Dual is X",
          dummy == "X");
      }

      // Explicitly no rs.close() to test close in constructor
    }

    conn->terminateStatement(stmt);
  }

public:

  void setUp() {
  }

  void tearDown() {
  }

  void testSmartOcciResultSet() {
    using namespace castor::tape;

    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect NULL statement",
      castor::db::ora::SmartOcciResultSet rs((oracle::occi::Statement *)NULL,
        (oracle::occi::ResultSet *)1),
      castor::exception::Exception);

    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to detect NULL result-set",
      castor::db::ora::SmartOcciResultSet rs((oracle::occi::Statement *)1,
        (oracle::occi::ResultSet *)NULL), castor::exception::Exception);

    // Get the connection details of the stager database
    std::list<std::string> configLines;
    utils::parseFileList("/etc/castor/ORASTAGERCONFIG", configLines);

    const std::string user   =
      getStagerDbConfigParam(configLines, "DbCnvSvc", "user"  );
    const std::string passwd =
      getStagerDbConfigParam(configLines, "DbCnvSvc", "passwd");
    const std::string dbName =
      getStagerDbConfigParam(configLines, "DbCnvSvc", "dbName");

    oracle::occi::Environment *const env =
      oracle::occi::Environment::createEnvironment();
    oracle::occi::Connection *conn =
      env->createConnection(user, passwd, dbName);

    {
      oracle::occi::Statement *stmt = conn->createStatement(
        "SELECT Dummy FROM Dual");
      {
        castor::db::ora::SmartOcciResultSet rs(stmt, stmt->executeQuery());

        while(rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
          std::string dummy = rs->getString(1);
          CPPUNIT_ASSERT_MESSAGE("Checking Dummy from Dual is X",
            dummy == "X");
        }

        CPPUNIT_ASSERT_NO_THROW_MESSAGE("Test SmartOcciResultSet::close()",
          rs.close());
      }

      conn->terminateStatement(stmt);
    }

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Test close in SmartOcciResultSet::~SmartOcciResultSet()",
      closeInSmartOcciResultSetDestructor(conn));

    env->terminateConnection(conn);
    oracle::occi::Environment::terminateEnvironment(env);
  }

  CPPUNIT_TEST_SUITE(OraTest);
  CPPUNIT_TEST(testSmartOcciResultSet);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(OraTest);

#endif // TEST_CASTOR_DB_ORA_ORATEST_HPP
