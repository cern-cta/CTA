/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace cta::tape::daemon {

/** The structure representing the underfill detection limits (e.g. ArchiveFetchUnderfillLimits)
 *  for mount request batches, i.e. the watch period in seconds underfillWatchPeriodSecs,
 *  minimum number of underfilled batches underfillMinSamples, start threshold
 *  underfillStartThreshold [0-100] and a underfillRecoveryThreshold [0-100]. When **cta-taped**
 *  repeatedly receives archive request batches, the requested number of
 *  files and bytes is defined in e.g. ArchiveFetchBytesFiles parameter. When the
 *  effective fill ratio (max(filesFetched/filesRequested, bytesFetched/bytesRequested)
 *  remains below the configured thresholds, the daemon concludes that the backend
 *  cannot supply enough work to efficiently keep the tape drive busy and ends the tape session.
 *  An underfill observation period starts when the effective fill ratio
 *  falls below the start threshold, ends when it reaches or exceeds
 *  the recovery threshold. If the measured period is longer than the
 *  configured watch period and the minimum number of underfilled fetched
 *  batches is reached, the end of the tape session is triggered.
 */
struct UnderfillFetchLimits {
  uint64_t underfillWatchPeriodSecs;
  uint64_t underfillMinSamples;
  uint64_t underfillStartThreshold;
  uint64_t underfillRecoveryThreshold;

  UnderfillFetchLimits()
      : underfillWatchPeriodSecs(0),
        underfillMinSamples(0),
        underfillStartThreshold(0),
        underfillRecoveryThreshold(0) {}

  UnderfillFetchLimits(uint64_t watchPeriodSecs,
                       uint64_t minSamples,
                       uint64_t startThreshold,
                       uint64_t recoveryThreshold)
      :

        underfillWatchPeriodSecs(watchPeriodSecs),
        underfillMinSamples(minSamples),
        underfillStartThreshold(startThreshold),
        underfillRecoveryThreshold(recoveryThreshold)
  {}
};

}  // namespace cta::tape::daemon
