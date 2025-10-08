/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RepackRequestPromotionStatistics.hpp"
#include "common/exception/Exception.hpp"

namespace cta::schedulerdb {

[[noreturn]]RepackRequestPromotionStatistics::RepackRequestPromotionStatistics()
{
   throw cta::exception::Exception("Not implemented");
}

SchedulerDatabase::RepackRequestStatistics::PromotionToToExpandResult RepackRequestPromotionStatistics::promotePendingRequestsForExpansion(size_t requestCount,
      log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

} // namespace cta::schedulerdb
