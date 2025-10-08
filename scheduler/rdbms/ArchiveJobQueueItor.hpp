/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "scheduler/rdbms/RelationalDB.hpp"
#include "common/dataStructures/ArchiveJob.hpp"

#include <string>

namespace cta::schedulerdb {

class ArchiveJobQueueItor : public SchedulerDatabase::IArchiveJobQueueItor {
 friend class cta::RelationalDB;

 public:

  [[noreturn]] ArchiveJobQueueItor();

   const std::string &qid() const override;

   bool end() const override;

   void operator++() override;

   const common::dataStructures::ArchiveJob &operator*() const override;
};

} // namespace cta::schedulerdb
