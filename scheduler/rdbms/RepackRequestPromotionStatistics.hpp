/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "scheduler/rdbms/RelationalDB.hpp"
#include "common/log/LogContext.hpp"

#include <cstddef>

namespace cta::schedulerdb {

class RepackRequestPromotionStatistics : public SchedulerDatabase::RepackRequestStatistics {
 friend class cta::RelationalDB;

 public:

   [[noreturn]] RepackRequestPromotionStatistics();

   PromotionToToExpandResult promotePendingRequestsForExpansion(size_t requestCount,
      log::LogContext &lc) override;
};

} // namespace cta::schedulerdb
