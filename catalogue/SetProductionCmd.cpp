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

#include "SetProductionCmd.hpp"
#include "rdbms/Login.hpp"
#include "SchemaChecker.hpp"
#include "SetProductionCmdLineArgs.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SetProductionCmd::SetProductionCmd(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream):
  CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int SetProductionCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const SetProductionCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }
  
  const rdbms::Login dbLogin = rdbms::Login::parseFile(cmdLineArgs.dbConfigPath);
  const uint64_t maxNbConns = 1;
  rdbms::ConnPool connPool(dbLogin, maxNbConns);
  auto conn = connPool.getConn();
  
  if(!isProductionSettable(dbLogin,conn)){
    throw cta::exception::Exception("Unable to set the catalogue as production because the column IS_PRODUCTION is missing");
  }
  
  m_out << "Setting the IS_PRODUCTION flag..." << std::endl;
  setProductionFlag(conn);
  m_out << "IS_PRODUCTION flag set." << std::endl;
  
  return 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void SetProductionCmd::printUsage(std::ostream &os) {
  SetProductionCmdLineArgs::printUsage(os);
}

//------------------------------------------------------------------------------
// isProductionSettable
//------------------------------------------------------------------------------
bool SetProductionCmd::isProductionSettable(const cta::rdbms::Login & login, cta::rdbms::Conn & conn) const {
  //Check that the IS_PRODUCTION column is there
  cta::catalogue::SchemaChecker::Builder builder("catalogue",login.dbType,conn);
  auto schemaChecker = builder.build();
  cta::catalogue::SchemaCheckerResult res = schemaChecker->checkTableContainsColumns("CTA_CATALOGUE",{"IS_PRODUCTION"});
  return (res.getStatus() == cta::catalogue::SchemaCheckerResult::Status::SUCCESS);
}

//------------------------------------------------------------------------------
// setProductionFlag
//------------------------------------------------------------------------------
void SetProductionCmd::setProductionFlag(cta::rdbms::Conn& conn) const {
  const char *  const sql = "UPDATE CTA_CATALOGUE SET IS_PRODUCTION='1'";
  try {
    conn.executeNonQuery(sql);
  } catch(const exception::Exception & ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}


} // namespace cta::catalogue
