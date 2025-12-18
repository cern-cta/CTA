/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RepackReportBatch.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::schedulerdb {

[[noreturn]] RepackReportBatch::RepackReportBatch() {
  throw cta::exception::NotImplementedException();
}

void RepackReportBatch::report(log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

}  // namespace cta::schedulerdb
