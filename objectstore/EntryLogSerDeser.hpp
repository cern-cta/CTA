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

#include "objectstore/cta.pb.h"
#include "common/dataStructures/EntryLog.hpp"

#include <string>
#include <stdint.h>
#include <limits>

namespace cta::objectstore {

/**
 * A decorator class of scheduler's creation log adding serialization.
 */
// TODO: generalize the mechanism to other structures.
class EntryLogSerDeser: public cta::common::dataStructures::EntryLog {
public:
  EntryLogSerDeser (): cta::common::dataStructures::EntryLog() {}
  explicit EntryLogSerDeser(const cta::common::dataStructures::EntryLog& el) : cta::common::dataStructures::EntryLog(el) {}
  EntryLogSerDeser (const std::string & un, const std::string & hn, uint64_t t): cta::common::dataStructures::EntryLog() {
    username=un;
    host=hn;
    time=t;
  }

  void serialize (cta::objectstore::serializers::EntryLog & log) const {
    log.set_username(username);
    log.set_host(host);
    log.set_time(time);
  }
  void deserialize (const cta::objectstore::serializers::EntryLog & log) {
    username=log.username();
    host = log.host();
    time  = log.time();
  }
};
  
} // namespace cta::objectstore
