/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <string>

#include "EntryLog.hpp"

namespace cta::common::dataStructures {

struct VirtualOrganization {
  /**
   * The name
   */
  std::string name;
  /**
   * The comment.
   */
  std::string comment;
  /**
   * Maximum number of drives allocated for reading per VO
   */
  uint64_t readMaxDrives;
  /**
   * Max number of drives allocated for writing per VO
   */
  uint64_t writeMaxDrives;

  /**
   * Max size of files belonging to VO
   */
  uint64_t maxFileSize;

  /**
   * The creation log.
   */
  EntryLog creationLog;

  /**
   * The last modification log.
   */
  EntryLog lastModificationLog;

  /**
   * The disk instance name.
   */
  std::string diskInstanceName;

  /**
   * True if this is the default repack VO
   */
  bool isRepackVo;

  bool operator==(const VirtualOrganization & other) const{
    return (
            name == other.name
            && comment == other.comment
            && readMaxDrives == other.readMaxDrives
            && writeMaxDrives == other.writeMaxDrives
            && maxFileSize == other.maxFileSize
            && diskInstanceName == other.diskInstanceName
            && isRepackVo == other.isRepackVo);
  }
};

} // namespace cta::common::dataStructures
