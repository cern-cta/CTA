/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/ArchiveJobQueueItor.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::schedulerdb {

[[noreturn]] ArchiveJobQueueItor::ArchiveJobQueueItor() {
  throw cta::exception::NotImplementedException();
}

const std::string& ArchiveJobQueueItor::qid() const {
  throw cta::exception::NotImplementedException();
}

bool ArchiveJobQueueItor::end() const {
  throw cta::exception::NotImplementedException();
}

void ArchiveJobQueueItor::operator++() {
  throw cta::exception::NotImplementedException();
}

const common::dataStructures::ArchiveJob& ArchiveJobQueueItor::operator*() const {
  throw cta::exception::NotImplementedException();
}

}  // namespace cta::schedulerdb
