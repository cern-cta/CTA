/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "RetrieveJobQueueItor.hpp"
#include "common/exception/Exception.hpp"

namespace cta::schedulerdb {

[[noreturn]] RetrieveJobQueueItor::RetrieveJobQueueItor() {
  throw cta::exception::Exception("Not implemented");
}

const std::string& RetrieveJobQueueItor::qid() const {
  throw cta::exception::Exception("Not implemented");
}

bool RetrieveJobQueueItor::end() const {
  throw cta::exception::Exception("Not implemented");
}

void RetrieveJobQueueItor::operator++() {
  throw cta::exception::Exception("Not implemented");
}

const common::dataStructures::RetrieveJob& RetrieveJobQueueItor::operator*() const {
  throw cta::exception::Exception("Not implemented");
}

}  // namespace cta::schedulerdb
