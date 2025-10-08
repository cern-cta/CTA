/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <gtest/gtest.h>

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "common/log/DummyLogger.hpp"

namespace unitTests {

class cta_catalogue_SchemaTest : public ::testing::TestWithParam<cta::catalogue::CatalogueFactory **> {
public:
  cta_catalogue_SchemaTest();

  void SetUp() override;
  void TearDown() override;

protected:
  cta::log::DummyLogger m_dummyLog;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
};

}  // namespace unitTests
