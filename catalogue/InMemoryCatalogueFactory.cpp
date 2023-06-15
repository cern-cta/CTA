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

#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/InMemoryCatalogueFactory.hpp"
#include "catalogue/retrywrappers/CatalogueRetryWrapper.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
InMemoryCatalogueFactory::InMemoryCatalogueFactory(log::Logger& log,
                                                   const uint64_t nbConns,
                                                   const uint64_t nbArchiveFileListingConns,
                                                   const uint32_t maxTriesToConnect) :
m_log(log),
m_nbConns(nbConns),
m_nbArchiveFileListingConns(nbArchiveFileListingConns),
m_maxTriesToConnect(maxTriesToConnect) {}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<Catalogue> InMemoryCatalogueFactory::create() {
  try {
    auto c = std::make_unique<InMemoryCatalogue>(m_log, m_nbConns, m_nbArchiveFileListingConns);
    return std::make_unique<CatalogueRetryWrapper>(m_log, std::move(c), m_maxTriesToConnect);
  }
  catch (exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

}  // namespace catalogue
}  // namespace cta
