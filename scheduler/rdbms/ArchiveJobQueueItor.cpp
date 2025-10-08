/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/ArchiveJobQueueItor.hpp"
#include "common/exception/Exception.hpp"

namespace cta::schedulerdb {

  [[noreturn]] ArchiveJobQueueItor::ArchiveJobQueueItor()
  {
    throw cta::exception::Exception("Not implemented");
  }

  const std::string &ArchiveJobQueueItor::qid() const
  {
     throw cta::exception::Exception("Not implemented");
  }

  bool ArchiveJobQueueItor::end() const
  {
     throw cta::exception::Exception("Not implemented");
  }

  void ArchiveJobQueueItor::operator++()
  {
     throw cta::exception::Exception("Not implemented");
  }

  const common::dataStructures::ArchiveJob &ArchiveJobQueueItor::operator*() const
  {
     throw cta::exception::Exception("Not implemented");
  }

} // namespace cta::schedulerdb
