/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/MediaType.hpp"
#include "common/dataStructures/EntryLog.hpp"

#include <cstdint>
#include <string>

namespace cta::catalogue {

/**
 * Structure describing a tape media type together with cfeation and last
 * modification logs.
 */
struct MediaTypeWithLogs: public MediaType {

  /**
   * The creation log.
   */
  common::dataStructures::EntryLog creationLog;

  /**
   * The last modification log.
   */
  common::dataStructures::EntryLog lastModificationLog;

}; // struct MediaType

} // namespace cta::catalogue
