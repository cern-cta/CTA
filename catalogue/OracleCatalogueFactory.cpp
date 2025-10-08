/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
    auto c = std::make_unique<OracleCatalogue>(m_log, m_login, m_nbConns, m_nbArchiveFileListingConns);
    return std::make_unique<CatalogueRetryWrapper>(m_log, std::move(c), m_maxTriesToConnect);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::catalogue

extern "C" {

void factory(
  cta::plugin::Interface<
    cta::catalogue::CatalogueFactory,
    cta::plugin::Args<cta::log::Logger&, const u_int64_t, const u_int64_t, const u_int64_t>,
    cta::plugin::Args<cta::log::Logger&, const cta::rdbms::Login&, const u_int64_t, const u_int64_t, const u_int64_t>>&
    interface) {
  interface.SET<cta::plugin::DATA::PLUGIN_NAME>("ctacatalogueocci")
    .SET<cta::plugin::DATA::API_VERSION>(VERSION_API)
    .CLASS<cta::catalogue::OracleCatalogueFactory>("OracleCatalogueFactory");
}

}// extern "C"
