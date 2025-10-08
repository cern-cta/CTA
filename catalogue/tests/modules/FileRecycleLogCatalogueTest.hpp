/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <gtest/gtest.h>

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/dataStructures/StorageClass.hpp"

namespace unitTests {

class cta_catalogue_FileRecycleLogTest : public ::testing::TestWithParam<cta::catalogue::CatalogueFactory **> {
public:
  cta_catalogue_FileRecycleLogTest();

  void SetUp() override;
  void TearDown() override;

protected:
  cta::log::DummyLogger m_dummyLog;
  const cta::catalogue::CreateTapeAttributes m_tape1;
  const cta::catalogue::CreateTapeAttributes m_tape2;
  const cta::catalogue::CreateTapeAttributes m_tape3;
  const cta::catalogue::MediaType m_mediaType;
  const cta::common::dataStructures::SecurityIdentity m_admin;
  const cta::common::dataStructures::DiskInstance m_diskInstance;
  const cta::common::dataStructures::VirtualOrganization m_vo;
  const cta::common::dataStructures::StorageClass m_storageClassSingleCopy;
  const cta::common::dataStructures::StorageClass m_storageClassDualCopy;
  const cta::common::dataStructures::StorageClass m_storageClassTripleCopy;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
};

}  // namespace unitTests
