/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#pragma once

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace cta::telemetry::metrics {

/**
 * Amount of work done by a pass of the disk report runner.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdDiskReporterCount;

/**
 * Amount of work done by a pass of the queue cleanup runner.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdQueueCleanupCount;

/**
 * Amount of work done by a pass of the garbage collector runner.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdGarbageCollectorCount;
/**
 * Amount of work reported by the repack request runner in one pass.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdRepackReportingCount;

/**
 * Number of retrieval jobs created by the expansion of repack repack requests
 * in one pass of the maintenance runner.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaMaintdRepackExpansionCount;

}  // namespace cta::telemetry::metrics
