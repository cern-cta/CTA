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

#pragma once

#include <xroot_plugins/XrdCtaStream.hpp>
#include <xroot_plugins/XrdSsiCtaRequestMessage.hpp>


namespace cta { namespace xrd {

class XrdCtaChangeStorageClass {
public:
  XrdCtaChangeStorageClass(cta::catalogue::Catalogue &catalogue, cta::log::LogContext &lc);
  void updateCatalogue(const std::vector<std::basic_string<char>>& archiveFileIds, const std::string& newStorageClassName);
private:
  cta::catalogue::Catalogue &m_catalogue;
  cta::log::LogContext &m_lc;
};

XrdCtaChangeStorageClass::XrdCtaChangeStorageClass(cta::catalogue::Catalogue &catalogue, cta::log::LogContext &lc): m_catalogue(catalogue), m_lc(lc) {}

void XrdCtaChangeStorageClass::updateCatalogue(const std::vector<std::basic_string<char>>& archiveFileIds, const std::string& newStorageClassName) {
  for (auto & id : archiveFileIds) {
    const uint64_t archiveFileId = std::stoul(id);
    m_catalogue.ArchiveFile()->modifyArchiveFileStorageClassId(archiveFileId, newStorageClassName);
  }
}
} // namespace xrd
} // namespace cta