/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/InMemoryCatalogueFactory.hpp"

#include "catalogue/InMemoryCatalogue.hpp"
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
InMemoryCatalogueFactory::InMemoryCatalogueFactory(log::Logger& log,
                                                   const uint64_t nbConns,
                                                   const uint64_t nbArchiveFileListingConns,
                                                   const uint32_t maxTriesToConnect)
    : m_log(log),
      m_nbConns(nbConns),
      m_nbArchiveFileListingConns(nbArchiveFileListingConns),
      m_maxTriesToConnect(maxTriesToConnect) {}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<Catalogue> InMemoryCatalogueFactory::create() {
  auto c = std::make_unique<InMemoryCatalogue>(m_log, m_nbConns, m_nbArchiveFileListingConns);
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
  interface.SET<cta::plugin::DATA::PLUGIN_NAME>("ctacatalogueinmemory")
    .SET<cta::plugin::DATA::API_VERSION>(VERSION_API)
    .CLASS<cta::catalogue::InMemoryCatalogueFactory>("InMemoryCatalogueFactory");
}

}  // extern "C"
