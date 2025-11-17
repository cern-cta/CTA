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
#include "maintd/IRoutine.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintd {

class Scheduler;

class RepackReportRoutine : public IRoutine {
public:
  RepackReportRoutine(cta::log::LogContext &lc, cta::Scheduler &scheduler, int timeout);

  void execute() override final;
  std::string getName() const override final;

private:
  template<typename GetBatchFunc>
  void reportBatch(std::string_view reportingType, GetBatchFunc getBatchFunc) const;

  cta::log::LogContext& m_lc;
  cta::Scheduler & m_scheduler;
  int m_softTimeout;
};
} // namespace cta::maintd
