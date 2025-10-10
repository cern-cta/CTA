/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "catalogue/interfaces/SchemaCatalogue.hpp"

namespace cta::catalogue {

class Catalogue;
class SchemaVersion;

class SchemaCatalogueRetryWrapper: public SchemaCatalogue {
public:
  SchemaCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);
  ~SchemaCatalogueRetryWrapper() override = default;

  SchemaVersion getSchemaVersion() const override;

  void verifySchemaVersion() override;

  void ping() override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};  // class SchemaCatalogueRetryWrapper

} // namespace cta::catalogue
