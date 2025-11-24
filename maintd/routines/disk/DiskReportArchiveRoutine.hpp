/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "disk/DiskReporterFactory.hpp"
#include "maintd/IRoutine.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintd {

class DiskReportArchiveRoutine : public IRoutine {
public:
  DiskReportArchiveRoutine(cta::log::LogContext& lc, cta::Scheduler& scheduler, int batchSize, int softTimeout);
  void execute() final;
  std::string getName() const final;

private:
  cta::log::LogContext& m_lc;
  cta::Scheduler& m_scheduler;
  cta::disk::DiskReporterFactory m_reporterFactory;

  int m_batchSize = 500;
  int m_softTimeout = 30;
};

}  // namespace cta::maintd
