/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JobPool.hpp"
#include "ArchiveRdbJob.hpp"
#include "RetrieveRdbJob.hpp"

namespace cta::schedulerdb {
// Explicit instantiation
template class JobPool<ArchiveRdbJob>;
template class JobPool<RetrieveRdbJob>;
}  // namespace cta::schedulerdb
