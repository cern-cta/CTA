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

#pragma once

#include <gtest/gtest.h>

#include <list>
#include <map>
#include <memory>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/log/DummyLogger.hpp"

namespace unitTests {

class cta_catalogue_MediaTypeTest : public ::testing::TestWithParam<cta::catalogue::CatalogueFactory**> {
public:
  cta_catalogue_MediaTypeTest();

  void SetUp() override;
  void TearDown() override;

protected:
  cta::log::DummyLogger m_dummyLog;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;

  const cta::common::dataStructures::SecurityIdentity m_admin;
  const cta::common::dataStructures::VirtualOrganization m_vo;
  const cta::common::dataStructures::DiskInstance m_diskInstance;
  const cta::catalogue::MediaType m_mediaType;
  const cta::catalogue::CreateTapeAttributes m_tape1;

  /**
   * Creates a map from tape meida type name to tape media type from the
   * specified list of tape media types.
   *
   * @param listOfMediaTypes The list of tape media types.
   * @return Map from tape media type name to tape media type.
   */
  std::map<std::string, cta::catalogue::MediaTypeWithLogs>
    mediaTypeWithLogsListToMap(const std::list<cta::catalogue::MediaTypeWithLogs>& listOfMediaTypes);
};

}  // namespace unitTests
