/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include "EntryLog.hpp"

namespace cta::common::dataStructures {

struct DiskInstance {

   /**
   * The name
   */
  std::string name;

  /**
   * The comment.
   */
  std::string comment;

  /**
   * The creation log.
   */
  EntryLog creationLog;

  /**
   * The last modification log.
   */
  EntryLog lastModificationLog;

  bool operator==(const DiskInstance & other) const{
    return (name == other.name && comment == other.comment);
  }
};

} // namespace cta::common::dataStructures
