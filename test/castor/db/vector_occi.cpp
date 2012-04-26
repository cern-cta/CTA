/*
 * Test program for OCCI development.
 * Compiles with: gcc -ggdb -I /usr/include/oracle/10.2.0.3/client/ -L/usr/lib64/oracle/10.2.0.3/client/lib64 -locci -lclntsh vector_occi.cpp -o vector_occi
 */

#include <occi.h>
#include <regex.h>
#include <errno.h>
#include <cstdlib>
#include <string>
#include <iostream>
#include <fstream>
#include <exception>
#include <memory>

namespace localUtils {
  class exception: public std::exception {
  public:
    explicit exception(){};
    explicit exception(const std::string &s):m_what(s){};
    virtual ~exception() throw(){};
    void append (const std::string& s) {
      m_what += s;
    }
    virtual const char * what() const throw () {
      return m_what.c_str();
    }
  private:
    std::string m_what;
  };
}; // namespace localUtils

namespace configParser {
typedef struct connectionParameters {
  std::string user, password, db;
} connectionParameters;

connectionParameters getConnectionParameters(void)
{
  // Open the configuration file
  std::string cfpath(std::string(getenv("HOME"))+"/private/ORASCRATCHCONFIG");
  std::ifstream cf (cfpath.c_str(), std::ios::in);
  if (!cf.is_open()) {
    localUtils::exception e(std::string("Failed to open config file (")+cfpath+"): "+strerror(errno));
    throw e;
  }
  // Prepare regular expressions
  regex_t regUser, regPw, regDb;
  int rc1, rc2, rc3;
  rc1 = regcomp (&regUser, "^DbCnvSvc[[:space:]]+user[[:space:]]+(.*)$"  , REG_EXTENDED);
  rc2 = regcomp (&regPw,   "^DbCnvSvc[[:space:]]+passwd[[:space:]]+(.*)$", REG_EXTENDED);
  rc3 = regcomp (&regDb,   "^DbCnvSvc[[:space:]]+dbName[[:space:]]+(.*)$", REG_EXTENDED);
  if (rc1 || rc2 || rc3 ) {
    const size_t bsz = 1000;
    char err1[bsz], err2[bsz], err3[bsz];
    regerror(rc1, &regUser, err1, bsz);
    regerror(rc2, &regPw,   err2, bsz);
    regerror(rc3, &regDb,   err3, bsz);
    localUtils::exception e(std::string("Failed to compile regexes: rc1=")
                            +err1+" rc2="+err2+" rc3="+err3);
    throw e;
  }
  connectionParameters ret;
  while (cf.good()){
    std::string l;
    getline(cf, l);
    regmatch_t matches [2];
    if (!(rc1 = regexec(&regUser, l.c_str(), 2, matches, 0)) && (matches[1].rm_so >= 0)) {
      ret.user = l.substr(matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so + 1);
    }
    if (!(rc2 = regexec(&regPw, l.c_str(), 2, matches, 0)) && (matches[1].rm_so >= 0)) {
      ret.password = l.substr(matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so + 1);
    }
    if (!(rc3 = regexec(&regDb, l.c_str(), 2, matches, 0)) && (matches[1].rm_so >= 0)) {
      ret.db = l.substr(matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so + 1);
    }
  }
  regfree(&regUser);
  regfree(&regPw);
  regfree(&regDb);
  return ret;
}
}; // namespace configParser

int main (void)
{
  try {
    // Get the connection parameters
    configParser::connectionParameters conPar = configParser::getConnectionParameters();
    // Connect to the DB
    std::auto_ptr<oracle::occi::Environment> env (oracle::occi::Environment::createEnvironment(oracle::occi::Environment::OBJECT));
    std::auto_ptr<oracle::occi::ConnectionPool> conPool (env->createConnectionPool(
        conPar.user, conPar.password, conPar.db));
    std::auto_ptr<oracle::occi::Connection> con(conPool->createConnection(conPar.user, conPar.password));
    // Create the statement
    std::auto_ptr<oracle::occi::Statement> stmt(con->createStatement("BEGIN OCCI_VECTOR.OCCI_VECTOR(:1,:2); END;"));
    // Prepare the data
    std::vector<oracle::occi::Number> numvec;
    std::vector<std::string> strvec;
    numvec.push_back(1);
    numvec.push_back(2);
    numvec.push_back(3);
    strvec.push_back("One");
    strvec.push_back("Two");
    strvec.push_back("Three");
    oracle::occi::setVector(stmt.get(),1, numvec, "numList");
    oracle::occi::setVector(stmt.get(),2, strvec, "textList");
    // Run the statement
    stmt->executeUpdate();
    // Clean up
    con->terminateStatement(stmt.release());
    conPool->terminateConnection(con.release());
    env->terminateConnectionPool(conPool.release());
    oracle::occi::Environment::terminateEnvironment(env.release());
  } catch (std::exception &e) {
    // Express problems.
    std::cerr << "Exception caught in main: " << e.what() << std::endl;
    return -1;
  }
  return 0;
}
