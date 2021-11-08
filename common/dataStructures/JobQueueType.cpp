/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "JobQueueType.hpp"

namespace cta {
namespace common {
namespace dataStructures {

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

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
