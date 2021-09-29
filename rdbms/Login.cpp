/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "Login.hpp"

#include <fstream>
#include <sstream>
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <regex>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// s_fileFormat
//------------------------------------------------------------------------------
const char *Login::s_fileFormat = "one of "
  "in_memory, "
  "oracle:username/password@database, "
  "sqlite:filename, "
  "mysql://<username>:<password>@<host>:<port>/<db_name> or "
  "postgresql:[connectinfo | URI]";

const std::string Login::s_hiddenPassword = "******";

const std::string Login::DbTypeAndConnectionDetails::in_memory = "in_memory";
const std::string Login::DbTypeAndConnectionDetails::oracle = "oracle";
const std::string Login::DbTypeAndConnectionDetails::sqlite = "sqlite";
const std::string Login::DbTypeAndConnectionDetails::mysql = "mysql";
const std::string Login::DbTypeAndConnectionDetails::postgresql = "postgresql";

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Login::Login():
  dbType(DBTYPE_NONE),
  port(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Login::Login(
  const DbType type,
  const std::string &user,
  const std::string &passwd,
  const std::string &db,
  const std::string &host,
  const uint16_t p):
  dbType(type),
  username(user),
  password(passwd),
  database(db),
  hostname(host),
  port(p) {
}

//------------------------------------------------------------------------------
// parseFile
//------------------------------------------------------------------------------
Login Login::parseFile(const std::string &filename) {
  try {
    std::ifstream file(filename);
    if (!file) {
      throw exception::Exception("Failed to open file");
    }

    return parseStream(file);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string("Failed to parse database login file " + filename + ": " +
      ex.getMessage().str()));
  }
}

//------------------------------------------------------------------------------
// parseStream
//------------------------------------------------------------------------------
Login Login::parseStream(std::istream &inputStream) {
  const std::list<std::string> lines = readNonEmptyLines(inputStream);

  if (1 != lines.size()) {
    throw exception::Exception("There should only be one and only one line containing a connection string");
  }

  const std::string connectionString = lines.front();

  return parseString(connectionString);
}


//------------------------------------------------------------------------------
// parseStream
//------------------------------------------------------------------------------
Login Login::parseString(const std::string &connectionString) {
  if (connectionString.empty()) {
    throw exception::Exception("Invalid connection string: Empty string");
  }
  
  const auto typeAndDetails = parseDbTypeAndConnectionDetails(connectionString);

  if(typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::in_memory) {
    return parseInMemory(typeAndDetails.connectionDetails);
  } else if(typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::oracle) {
    return parseOracle(typeAndDetails.connectionDetails);
  } else if(typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::sqlite) {
    return parseSqlite(typeAndDetails.connectionDetails);
  } else if(typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::mysql) {
    return parseMySql(typeAndDetails.connectionDetails);
  } else if(typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::postgresql) {
    return parsePostgresql(typeAndDetails.connectionDetails);
  }

  throw exception::Exception(std::string("Invalid connection string: Unknown database type ") +
    typeAndDetails.dbTypeStr);
}

//------------------------------------------------------------------------------
// parseDbTypeAndConnectionDetails
//------------------------------------------------------------------------------
Login::DbTypeAndConnectionDetails Login::parseDbTypeAndConnectionDetails(const std::string &connectionString) {
  DbTypeAndConnectionDetails dbTypeAndConnectionDetails;

  // Parsing "databaseType:connectionDetails"
  std::string::size_type firstColonPos = connectionString.find(':');
  dbTypeAndConnectionDetails.dbTypeStr = connectionString.substr(0, firstColonPos);
  if (std::string::npos != firstColonPos && (connectionString.length() - 1) > firstColonPos) {
    dbTypeAndConnectionDetails.connectionDetails = connectionString.substr(firstColonPos + 1);
  }

  return dbTypeAndConnectionDetails;
}

//------------------------------------------------------------------------------
// readNonEmptyLines
//------------------------------------------------------------------------------
std::list<std::string> Login::readNonEmptyLines(std::istream &inputStream) {

  std::list<std::string> lines;
  std::string line;

  while(std::getline(inputStream, line)) {
    // Remove the newline character if there is one
    {
      const std::string::size_type newlinePos = line.find("\n");
      if(newlinePos != std::string::npos) {
        line = line.substr(0, newlinePos);
      }
    }

    // Left and right trim the line of whitespace
    line = utils::trimString(std::string(line));

    // Please note:
    //
    // Comments at the end of lines are not supported.
    // Ignoring whitespace, lines starting with '#' are comments.

    // If the line is not empty and is not a comment
    if(!line.empty() && line.at(0) != '#') {
      lines.push_back(line);
    }
  }

  return lines;
}

//------------------------------------------------------------------------------
// parseInMemory
//------------------------------------------------------------------------------
Login Login::parseInMemory(const std::string &connectionDetails) {
  if(!connectionDetails.empty()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  Login login(DBTYPE_IN_MEMORY, "", "", "", "", 0);
  login.setInMemoryConnectionString();
  return login;
}

void Login::setInMemoryConnectionString(){
  connectionString = Login::DbTypeAndConnectionDetails::in_memory;
}

//------------------------------------------------------------------------------
// parseOracle
//------------------------------------------------------------------------------
Login Login::parseOracle(const std::string &connectionDetails) {
  std::vector<std::string> userPassAndDbTokens;
  utils::splitString(connectionDetails, '@', userPassAndDbTokens);
  if(2 != userPassAndDbTokens.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  const std::string &userAndPass = userPassAndDbTokens[0];
  const std::string &db = userPassAndDbTokens[1];

  std::vector<std::string> userAndPassTokens;
  utils::splitString(userAndPass, '/', userAndPassTokens);
  if(2 != userAndPassTokens.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  const std::string &user = userAndPassTokens[0];
  const std::string &pass = userAndPassTokens[1];

  Login login(DBTYPE_ORACLE, user, pass, db, "", 0);
  login.setOracleConnectionString(user,db);
  return login;
}

void Login::setOracleConnectionString(const std::string & user, const std::string & db){
  connectionString = Login::DbTypeAndConnectionDetails::oracle+":"+user+"/"+s_hiddenPassword+"@"+db;
}

//------------------------------------------------------------------------------
// parseSqlite
//------------------------------------------------------------------------------
Login Login::parseSqlite(const std::string &connectionDetails) {
  const std::string &filename = connectionDetails;

  if(filename.empty()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  
  Login login(DBTYPE_SQLITE, "", "", filename, "", 0);
  login.setSqliteConnectionString(filename);
  return login;
}

void Login::setSqliteConnectionString(const std::string & filename){
  connectionString = Login::DbTypeAndConnectionDetails::sqlite+":"+filename;
}

//------------------------------------------------------------------------------
// parseMySql
//------------------------------------------------------------------------------
Login Login::parseMySql(const std::string &connectionDetails) {
  // The full url pattern:
  //     mysql://<username>:<password>@<host>:<port>/<db_name>
  //
  // optional: 
  //   - <username>:<password>@ 
  //   - <password>
  //   - <port>
  //
  // Note:
  //   - hostname can also be ipv4 or ipv6 address.
  //     For ipv6, note hostname is string between "[" and "]", then is the port
  

  std::string username;
  std::string password;
  std::string host;
  uint16_t port = 3306;
  std::string database;
  // connectionDetails is //<username>:<password>@<host>:<port>/<db_name>

  const std::string protocol = "//";
  const size_t protocol_sz = protocol.size();
  if (connectionDetails.substr(0, protocol.size()) != protocol) {
    throw exception::Exception(std::string(__FUNCTION__) + " mysql url format wrong.");
  }

  const std::string host_sep = "@"; // first "@" or optional 
  auto idx_sep = connectionDetails.find(host_sep);

  bool has_usr_pwd = true;
  std::string usr_pwd_str;
  std::string host_port_db_str;
  // if "@" is not found, that means no user/password
  // the password can be in ~/.my.cnf file
  if (idx_sep == std::string::npos) {
    has_usr_pwd = false;
    host_port_db_str = connectionDetails.substr(protocol_sz);
  } else if (idx_sep+1 < connectionDetails.size()) {
    usr_pwd_str = connectionDetails.substr(protocol_sz, idx_sep-protocol_sz);
    host_port_db_str = connectionDetails.substr(idx_sep+1);
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + " empty url.");
  }

  // parse username and password 
  if (has_usr_pwd) {
    const std::string pass_sep = ":";

    auto idx_pass_sep = usr_pwd_str.find(pass_sep);
    username = usr_pwd_str.substr(0, idx_pass_sep);
    if (idx_pass_sep != std::string::npos
        && (idx_pass_sep+1) < usr_pwd_str.size()) {
      password = usr_pwd_str.substr(idx_pass_sep+1);
    }

  }

  // parse host, port and db name
  //   host and db name should not be optional
  const std::string db_sep = "/";
  auto idx_db_sep = host_port_db_str.find(db_sep);

  if (idx_db_sep == std::string::npos) {
    throw exception::Exception(std::string(__FUNCTION__) + " specify host/dbname");
  } else if (idx_db_sep+1 >= host_port_db_str.size()) {
    throw exception::Exception(std::string(__FUNCTION__) + " specify host/dbname");
  }

  const std::string host_port_str = host_port_db_str.substr(0, idx_db_sep);
  const std::string db_str = host_port_db_str.substr(idx_db_sep+1);

  if (host_port_str.empty()) {
      throw exception::Exception(std::string(__FUNCTION__) + " host is missing");
  }

  if (db_str.empty()) {
      throw exception::Exception(std::string(__FUNCTION__) + " database is missing");
  }


  // first part: host:port
  const std::string port_sep = ":";
  const std::string ipv6_sep_begin = "[";
  const std::string ipv6_sep_end = "]";
  size_t idx_port_sep = 0;

  if (host_port_str.substr(0, 1) == ipv6_sep_begin) {
    // ipv6 addr
    idx_port_sep = host_port_str.find(ipv6_sep_end);

    // invalid ipv6
    if (idx_port_sep == std::string::npos // missing "]"
        or idx_port_sep == 1// empty hostname, such as "[]"
        ) {
      throw exception::Exception(std::string(__FUNCTION__) + " invalid ipv6 addr " + host_port_str);
    }

    idx_port_sep = idx_port_sep  + 1;

  } else {
    idx_port_sep = host_port_str.find(port_sep);
  }

  std::string port_str;
  host = host_port_str.substr(0, idx_port_sep);
  if (idx_port_sep == std::string::npos 
      or idx_port_sep+1 >= host_port_str.size()) {
    // nop: port is omit
  } else {
    port_str = host_port_str.substr(idx_port_sep+1);

    // convert number
    std::stringstream ss;
    ss << port_str;
    ss >> port;

    if (ss.fail()) {
      throw exception::Exception(std::string(__FUNCTION__) + " invalid port " + port_str);
    }
  }

  // second part db name
  database = db_str;  
  
  Login login(DBTYPE_MYSQL, username, password, database, host, port);
  login.setMySqlConnectionString(username,password,host,port_str,database);
  return login;
}

void Login::setMySqlConnectionString(const std::string & pUsername, const std::string & pPassword,const std::string & pHost, const std::string & pPort, const std::string & pDatabase) {
  //mysql://<username>:<password>@<host>:<port>/<db_name>
  // optional: 
  //   - <username>:<password>@ 
  //   - <password>
  //   - <port>
  connectionString = DbTypeAndConnectionDetails::mysql+"://";
  
  bool hasUsername = !pUsername.empty();
  bool hasPassword = !pPassword.empty();
  bool hasPort = !pPort.empty();

  connectionString += (hasUsername ? pUsername : "") + (hasPassword ? ":" + s_hiddenPassword + "@" : (hasUsername ? "@" : "")) + pHost + (hasPort ? ":" + pPort : "") + "/" + pDatabase; 
}

//------------------------------------------------------------------------------
// parsePostgresql
//------------------------------------------------------------------------------
Login Login::parsePostgresql(const std::string &connectionDetails) {
  Login login(DBTYPE_POSTGRESQL, "", "", connectionDetails, "", 0);
  login.setPostgresqlConnectionString(connectionDetails);
  return login;
}

void Login::setPostgresqlConnectionString(const std::string& connectionDetails){
  connectionString = Login::DbTypeAndConnectionDetails::postgresql+":";
  if(!postgresqlHasPassword(connectionDetails)){
    //No password displayed so no need to hide it
    connectionString += connectionDetails;
  } else {
    cta::utils::Regex regex2("(postgresql://.*:)(.*)(@.*)");
    std::vector<std::string> result2 = regex2.exec(connectionDetails);
    connectionString += result2[1] + s_hiddenPassword + result2[3];
  }
}

bool Login::postgresqlHasPassword(const std::string& connectionDetails){
  //https://www.postgresql.org/docs/10/libpq-connect.html
  if(connectionDetails.find("@") == std::string::npos){
    return false;
  }
  cta::utils::Regex regex("postgresql://(.*)@");
  std::vector<std::string> result = regex.exec(connectionDetails);
  std::string usernamePassword = result[1];
  if(usernamePassword.find(":") == std::string::npos){
    //No password provided, no need to hide it
    return false;
  }
  return true;
}

} // namespace catalogue
} // namespace cta
