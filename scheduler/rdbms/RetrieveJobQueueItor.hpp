/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RelationalDB.hpp"
#include "common/dataStructures/RetrieveJob.hpp"

#include <string>

namespace cta::schedulerdb {

class RetrieveJobQueueItor : public SchedulerDatabase::IRetrieveJobQueueItor {
  friend class cta::RelationalDB;

public:
  [[noreturn]] RetrieveJobQueueItor();

  const std::string& qid() const override;

  bool end() const override;

  void operator++() override;

  const common::dataStructures::RetrieveJob& operator*() const override;
};

}  // namespace cta::schedulerdb
