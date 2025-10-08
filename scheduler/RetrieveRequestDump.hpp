/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/CreationLog.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/TapeFile.hpp"

#include <list>
#include <string>

namespace cta {

/**
 * Class representing a user request to retrieve a single archive file to a
 * single remote file.
 */
struct RetrieveRequestDump {
  cta::common::dataStructures::RetrieveRequest retrieveRequest; /**< The full path of the source archive file. */
  cta::common::dataStructures::RetrieveFileQueueCriteria criteria; /**< The list of tape files and mount criteria */
  uint64_t activeCopyNb; /**< The tape copy number currenty considered for retrieve. */
}; // struct RetrieveFromTapeCopyRequest

} // namespace cta
