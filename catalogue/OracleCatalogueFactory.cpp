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

#include <memory>
#include <string>
#include <utility>

#include "catalogue/OracleCatalogueFactory.hpp"
#include "catalogue/rdbms/oracle/OracleCatalogue.hpp"
#include "catalogue/retrywrappers/CatalogueRetryWrapper.hpp"
#include "common/exception/Exception.hpp"
#include "plugin-manager/PluginInterface.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OracleCatalogueFactory::OracleCatalogueFactory(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns,
  const uint32_t maxTriesToConnect):
  m_log(log),
  m_login(login),
  m_nbConns(nbConns),
  m_nbArchiveFileListingConns(nbArchiveFileListingConns),
  m_maxTriesToConnect(maxTriesToConnect) {
  if (rdbms::Login::DBTYPE_ORACLE != login.dbType) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << "failed: Incorrect database type: expected=DBTYPE_ORACLE actual=" <<
      rdbms::Login::dbTypeToString(login.dbType);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<Catalogue> OracleCatalogueFactory::create() {
  try {
    auto c = std::make_unique<OracleCatalogue>(m_log, m_login.username, m_login.password, m_login.database, m_nbConns,
      m_nbArchiveFileListingConns);
    return std::make_unique<CatalogueRetryWrapper>(m_log, std::move(c), m_maxTriesToConnect);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::catalogue

extern "C" {

void factory(cta::plugin::Interface<cta::catalogue::CatalogueFactory,
    cta::plugin::Args<
      cta::log::Logger&,
      const cta::rdbms::Login&, const u_int64_t,
      const u_int64_t, const u_int64_t>>& interface) {
  
  interface.SET<cta::plugin::DATA::PLUGIN_NAME>("ctacatalogueocci")
    .CLASS<cta::catalogue::OracleCatalogueFactory>("OracleCatalogueFactory");
}

}// extern "C"
