/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/SchemaCatalogue.hpp"

namespace cta::catalogue {

class DummySchemaCatalogue: public SchemaCatalogue {
public:
  DummySchemaCatalogue() = default;
  ~DummySchemaCatalogue() override = default;

  SchemaVersion getSchemaVersion() const override;

  void verifySchemaVersion() override;

  void ping() override;
};

} // namespace cta::catalogue
