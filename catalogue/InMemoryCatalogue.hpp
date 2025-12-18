/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"

namespace cta::catalogue {

class CatalogueFactory;

/**
 * CTA catalogue class to be used for unit testing.
 */
class InMemoryCatalogue : public SchemaCreatingSqliteCatalogue {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  InMemoryCatalogue(log::Logger& log, const uint64_t nbConns, const uint64_t nbArchiveFileListingConns);
};

}  // namespace cta::catalogue
