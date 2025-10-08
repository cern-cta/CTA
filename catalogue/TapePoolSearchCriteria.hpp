/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <string>
#include <vector>

namespace cta::catalogue {

/**
 * The collection of criteria used to select a set of tapepools.
 *
 * A tapepool is selected if it meets all of the specified criteria.
 *
 * A criterion is only considered specified if it has been set.
 *
 * Please note that no wild cards, for example '*' or '%', are supported.
 */
struct TapePoolSearchCriteria {

   /**
   * The name of the tapepool.
   */
    std::optional<std::string> name;

    /**
    * The virtual organization of the tapepool.
    */
    std::optional<std::string> vo;

    /**
    * Set to true if searching for encrypted tape pools.
    */
    std::optional<bool> encrypted;

    /**
    * The encryption key name.
    */
    std::optional<std::string> encryptionKeyName;

}; // struct TapePoolSearchCriteria

} // namespace cta::catalogue
