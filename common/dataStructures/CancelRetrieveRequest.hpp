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
 * This is the request to cancel and ongoing retrieval
 */
struct CancelRetrieveRequest {

  CancelRetrieveRequest();

  bool operator==(const CancelRetrieveRequest &rhs) const;

  bool operator!=(const CancelRetrieveRequest &rhs) const;

  RequesterIdentity requester;
  uint64_t archiveFileID;
  std::string dstURL;
  DiskFileInfo diskFileInfo;
  std::string retrieveRequestId;

}; // struct CancelRetrieveRequest

std::ostream &operator<<(std::ostream &os, const CancelRetrieveRequest &obj);

} // namespace cta::common::dataStructures
