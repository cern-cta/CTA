/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::common::dataStructures {

enum class JobQueueType {
  JobsToTransferForUser,
  FailedJobs,
  JobsToReportToUser,
  JobsToReportToRepackForSuccess,
  JobsToReportToRepackForFailure,
  JobsToTransferForRepack
};

static const JobQueueType AllJobQueueTypes[] = {JobQueueType::JobsToTransferForUser,
                                                JobQueueType::FailedJobs,
                                                JobQueueType::JobsToReportToUser,
                                                JobQueueType::JobsToReportToRepackForSuccess,
                                                JobQueueType::JobsToReportToRepackForFailure,
                                                JobQueueType::JobsToTransferForRepack};

std::string toString(JobQueueType queueType);

}  // namespace cta::common::dataStructures
