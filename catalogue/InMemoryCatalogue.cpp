/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/InMemoryCatalogue.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
InMemoryCatalogue::InMemoryCatalogue(log::Logger& log, const uint64_t nbConns, const uint64_t nbArchiveFileListingConns)
    : SchemaCreatingSqliteCatalogue(log, "file::memory:?cache=shared", nbConns, nbArchiveFileListingConns) {}

}  // namespace cta::catalogue
