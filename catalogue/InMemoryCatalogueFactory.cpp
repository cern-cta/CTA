/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include "catalogue/CatalogueRetryWrapper.hpp"
#include "catalogue/InMemoryCatalogueFactory.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
InMemoryCatalogueFactory::InMemoryCatalogueFactory(
  log::Logger &log,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns,
  const uint32_t maxTriesToConnect):
  m_log(log),
  m_nbConns(nbConns),
  m_nbArchiveFileListingConns(nbArchiveFileListingConns),
  m_maxTriesToConnect(maxTriesToConnect) {
}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<Catalogue> InMemoryCatalogueFactory::create() {
  try {
    auto c = cta::make_unique<InMemoryCatalogue>(m_log, m_nbConns, m_nbArchiveFileListingConns);
    return cta::make_unique<CatalogueRetryWrapper>(m_log, std::move(c), m_maxTriesToConnect);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
