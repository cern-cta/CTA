/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
