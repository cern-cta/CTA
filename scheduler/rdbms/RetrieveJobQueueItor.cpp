/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RetrieveJobQueueItor.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::schedulerdb {

[[noreturn]] RetrieveJobQueueItor::RetrieveJobQueueItor() {
  throw cta::exception::NotImplementedException();
}

const std::string& RetrieveJobQueueItor::qid() const {
  throw cta::exception::NotImplementedException();
}

bool RetrieveJobQueueItor::end() const {
  throw cta::exception::NotImplementedException();
}

void RetrieveJobQueueItor::operator++() {
  throw cta::exception::NotImplementedException();
}

const common::dataStructures::RetrieveJob& RetrieveJobQueueItor::operator*() const {
  throw cta::exception::NotImplementedException();
}

}  // namespace cta::schedulerdb
