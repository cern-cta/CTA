/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RepackRequestPromotionStatistics.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::schedulerdb {

[[noreturn]] RepackRequestPromotionStatistics::RepackRequestPromotionStatistics() {
  throw cta::exception::NotImplementedException();
}

SchedulerDatabase::RepackRequestStatistics::PromotionToToExpandResult
RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(size_t requestCount, log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

}  // namespace cta::schedulerdb
