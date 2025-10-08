/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"

namespace cta::common::dataStructures {

/**
 * This struct holds all the command line parameters of a CTA
 * UpdateFileStorageClass command
 */
struct UpdateFileStorageClassRequest {

  UpdateFileStorageClassRequest();

  bool operator==(const UpdateFileStorageClassRequest &rhs) const;

  bool operator!=(const UpdateFileStorageClassRequest &rhs) const;

  RequesterIdentity requester;
  uint64_t archiveFileID;
  std::string storageClass;
  DiskFileInfo diskFileInfo;

}; // struct UpdateFileStorageClassRequest

std::ostream &operator<<(std::ostream &os, const UpdateFileStorageClassRequest &obj);

} // namespace cta::common::dataStructures
