/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/DummyLogger.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_DiskInstanceTest : public ::testing::TestWithParam<cta::catalogue::CatalogueFactory**> {
public:
  cta_catalogue_DiskInstanceTest();

  void SetUp() override;
  void TearDown() override;

protected:
  cta::log::DummyLogger m_dummyLog;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;

  const cta::common::dataStructures::SecurityIdentity m_admin;
};

}  // namespace unitTests
