/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/dummy/DummyFileRecycleLogCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace cta {
namespace catalogue {

FileRecycleLogItor DummyFileRecycleLogCatalogue::getFileRecycleLogItor(
  const RecycleTapeFileSearchCriteria & searchCriteria) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyFileRecycleLogCatalogue::restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria,
  const std::string &newFid) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyFileRecycleLogCatalogue::deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace catalogue
} // namespace cta
