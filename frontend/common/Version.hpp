/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::frontend {

  /**
   * Structure to hold CTA versions
   */
  struct Version {
    /**
     * CTA version major.minor
     */
    std::string ctaVersion;

    /**
     * xrootd-ssi-protobuf-interface version/tag
     */
    std::string protobufTag;
  };

} // namespace cta::frontend
