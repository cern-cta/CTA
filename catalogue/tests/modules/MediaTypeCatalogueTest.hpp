/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

class cta_catalogue_MediaTypeTest : public ::testing::TestWithParam<cta::catalogue::CatalogueFactory **> {
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
  std::map<std::string, cta::catalogue::MediaTypeWithLogs> mediaTypeWithLogsListToMap(
    const std::list<cta::catalogue::MediaTypeWithLogs> &listOfMediaTypes);
};

}  // namespace unitTests
