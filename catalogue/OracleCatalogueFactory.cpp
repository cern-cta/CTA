/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/OracleCatalogueFactory.hpp"

#include "catalogue/rdbms/oracle/OracleCatalogue.hpp"
#include "catalogue/retrywrappers/CatalogueRetryWrapper.hpp"
#include "common/exception/Exception.hpp"
#include "plugin-manager/PluginInterface.hpp"

#include <memory>
#include <string>
#include <utility>

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OracleCatalogueFactory::OracleCatalogueFactory(log::Logger& log,
                                               const rdbms::Login& login,
                                               const uint64_t nbConns,
                                               const uint64_t nbArchiveFileListingConns,
                                               const uint32_t maxTriesToConnect)
    : m_log(log),
      m_login(login),
      m_nbConns(nbConns),
      m_nbArchiveFileListingConns(nbArchiveFileListingConns),
      m_maxTriesToConnect(maxTriesToConnect) {
  if (rdbms::Login::DBTYPE_ORACLE != login.dbType) {
    throw exception::Exception("Incorrect database type: expected=DBTYPE_ORACLE actual="
                               + rdbms::Login::dbTypeToString(login.dbType));
  }
}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<Catalogue> OracleCatalogueFactory::create() {
  auto c = std::make_unique<OracleCatalogue>(m_log, m_login, m_nbConns, m_nbArchiveFileListingConns);
  return std::make_unique<CatalogueRetryWrapper>(m_log, std::move(c), m_maxTriesToConnect);
}

}  // namespace cta::catalogue

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

}  // extern "C"
