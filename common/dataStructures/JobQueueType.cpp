/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JobQueueType.hpp"

namespace cta::common::dataStructures {

std::string toString(JobQueueType queueType) {
  switch (queueType) {
    case JobQueueType::FailedJobs:
      return "failedJobs";
    case JobQueueType::JobsToReportToUser:
      return "JobsToReportToUser";
    case JobQueueType::JobsToTransferForUser:
      return "jobsToTranfer";
    case JobQueueType::JobsToReportToRepackForSuccess:
      return "JobsToReportToRepackForSuccess";
    case JobQueueType::JobsToReportToRepackForFailure:
      return "JobsToReportToRepackForFailure";
    case JobQueueType::JobsToTransferForRepack:
      return "JobsToTransferForRepack";
    default:
      return "Unknown queue type.";
  }
}

std::string toString(QueueType queueType) {
  switch (queueType) {
    case QueueType::Pending:
      return "PENDING_QUEUE";
    case QueueType::Active:
      return "ACTIVE_QUEUE";
    case QueueType::Failed:
      return "FAILED_QUEUE";
    default:
      return "Unknown queue type.";
  }
}

}  // namespace cta::common::dataStructures
