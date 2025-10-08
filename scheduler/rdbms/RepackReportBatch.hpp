/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "scheduler/rdbms/RelationalDB.hpp"
#include "common/log/LogContext.hpp"

namespace cta::schedulerdb {

class RepackReportBatch : public SchedulerDatabase::RepackReportBatch {
 friend class cta::RelationalDB;

 public:

   [[noreturn]] RepackReportBatch();

   void report(log::LogContext & lc) override;
};

} // namespace cta::schedulerdb
