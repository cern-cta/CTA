/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RepackReportBatch.hpp"
#include "common/exception/Exception.hpp"

namespace cta::schedulerdb {

[[noreturn]] RepackReportBatch::RepackReportBatch()
{
   throw cta::exception::Exception("Not implemented");
}

void RepackReportBatch::report(log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

} // namespace cta::schedulerdb
