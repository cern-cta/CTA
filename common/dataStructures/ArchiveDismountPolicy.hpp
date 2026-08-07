/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace cta::common::dataStructures {

/** The fetch underfill criteria to unmount archive session "ArchiveDismountPolicy"
 *  This mechanism and configuration is only effective for Postgres Scheduler Backend Implementation and
 *    has no effect on the objectstore implementation !
 * { underfillWatchPeriodSecs, underfillMinSamples,
 *   underfillStartThreshold, underfillRecoveryThreshold }.
 *  When archive mount fetches batches of jobs, it tries to fetch configured maximum number of files and bytes.
 *  This set of criteria helps to define, when there are not enough jobs in the backend to fetch enough files
 *  from the backend and keep the drive busy (e.g. because they are being process by several drives or because
 *  the queueing form the user is slow). These criteria are:
 *  1) the watch period in seconds underfillWatchPeriodSecs for which the drive is allowed to stay mounted despite fetching
 *  less than optimal bunch of files.
 *  2) the minimum number of underfilled batches underfillMinSamples which the mount has to see befoe considering dismount
 *  3) the start threshold underfillStartThreshold [0-100] is a fill percentage for the currently feched bunch based on the maxima
 *  requested/configured by the operator in ArchiveFetchBytesFiles
 *  4) the end threshold underfillRecoveryThreshold [0-100] is a fill percentage for the currently fetched bunch based on the maxima
 *  requested/configured by the operator in ArchiveFetchBytesFiles
 *  The cta-taped repeatedly receives archive request batches, the requested number of
 * files and bytes is defined in e.g. ArchiveFetchBytesFiles parameter. When the
 * effective fill ratio (max(filesFetched/filesRequested, bytesFetched/bytesRequested)
 * remains below the configured thresholds, the daemon concludes that the backend
 * cannot supply enough work to efficiently keep the tape drive busy and ends the tape session.
 * An underfill observation period starts when the effective fill ratio
 *  falls below the start threshold, ends when it reaches or exceeds
 * the recovery threshold. If the measured period is longer than the
 * configured watch period and the minimum number of underfilled fetched
 * batches is reached, the end of the tape session is triggered.
 */
struct ArchiveDismountPolicy {
  uint64_t underfillWatchPeriodSecs;
  uint64_t underfillMinSamples;
  uint64_t underfillStartThreshold;
  uint64_t underfillRecoveryThreshold;

  ArchiveDismountPolicy()
      : underfillWatchPeriodSecs(0),
        underfillMinSamples(0),
        underfillStartThreshold(0),
        underfillRecoveryThreshold(0) {}

  ArchiveDismountPolicy(uint64_t watchPeriodSecs,
                        uint64_t minSamples,
                        uint64_t startThreshold,
                        uint64_t recoveryThreshold)
      :

        underfillWatchPeriodSecs(watchPeriodSecs),
        underfillMinSamples(minSamples),
        underfillStartThreshold(startThreshold),
        underfillRecoveryThreshold(recoveryThreshold) {}

  void set(uint64_t watchPeriodSecs, uint64_t minSamples, uint64_t startThreshold, uint64_t recoveryThreshold) {
    underfillWatchPeriodSecs = watchPeriodSecs;
    underfillMinSamples = minSamples;
    underfillStartThreshold = startThreshold;
    underfillRecoveryThreshold = recoveryThreshold;
  }
};

}  // namespace cta::common::dataStructures
