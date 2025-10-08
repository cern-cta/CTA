/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/checksum/ChecksumBlob.hpp"

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct holds all the command line parameters of a CTA archive command
 */
struct ArchiveRequest {

  ArchiveRequest();

  bool operator==(const ArchiveRequest &rhs) const;

  bool operator!=(const ArchiveRequest &rhs) const;

  RequesterIdentity requester;
  std::string diskFileID;

  std::string srcURL;
  uint64_t fileSize;
  checksum::ChecksumBlob checksumBlob;
  std::string storageClass;
  DiskFileInfo diskFileInfo;
  std::string archiveReportURL;
  std::string archiveErrorReportURL;
  EntryLog creationLog;

}; // struct ArchiveRequest

std::ostream &operator<<(std::ostream &os, const ArchiveRequest &obj);

} // namespace cta::common::dataStructures
