/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/RepackInfo.hpp"
#include "common/log/LogContext.hpp"
#include "scheduler/SchedulerDatabase.hpp"

namespace cta {

/**
 * Control structure for the RepackRequest.
 */
class RepackRequest {
  friend class Scheduler;

public:
  void expand();
  const cta::common::dataStructures::RepackInfo getRepackInfo() const;
  void fail();

protected:
  std::unique_ptr<cta::SchedulerDatabase::RepackRequest> m_dbReq;
};  // class RepackRequest

}  // namespace cta
