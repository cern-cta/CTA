/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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
#pragma once

#include <string>
#include <map>
#include <memory>

namespace cta {
namespace catalogue {  
  
class DbToSQLiteStatementTransformer {
public:
  DbToSQLiteStatementTransformer(const std::string &statement);
  virtual ~DbToSQLiteStatementTransformer();
  virtual std::string transform();
protected:
  std::string m_statement;
};

class CreateGlobalTempTableToSQLiteStatementTransformer: public DbToSQLiteStatementTransformer {
public:
  CreateGlobalTempTableToSQLiteStatementTransformer(const std::string &statement);
  std::string transform() override;
};

class DeleteStatementTransformer :public DbToSQLiteStatementTransformer {
public:
  DeleteStatementTransformer(const std::string &statement);
  std::string transform() override;
};

class DbToSQLiteStatementTransformerFactory {
  enum StatementType {
    CREATE_TABLE,
    CREATE_INDEX,
    CREATE_GLOBAL_TEMPORARY_TABLE,
    CREATE_SEQUENCE,
    INSERT_INTO_CTA_VERSION,
    SKIP
  };
private:
  static const std::map<std::string,StatementType> regexToStatementMap;
  static std::map<std::string,StatementType> initializeRegexToStatementMap();
  static StatementType statementToStatementType(const std::string &statement);
public:
  static std::unique_ptr<DbToSQLiteStatementTransformer> create(const std::string &statement);
};

}}