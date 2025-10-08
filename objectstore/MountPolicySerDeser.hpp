/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <limits>
#include <stdint.h>
#include <string>

#include "common/dataStructures/MountPolicy.hpp"
#include "EntryLogSerDeser.hpp"
#include "objectstore/cta.pb.h"

namespace cta::objectstore {

/**
 * A decorator class of scheduler's creation log adding serialization.
 */
class MountPolicySerDeser: public cta::common::dataStructures::MountPolicy {
public:
  MountPolicySerDeser (): cta::common::dataStructures::MountPolicy() {}
  explicit MountPolicySerDeser(const cta::common::dataStructures::MountPolicy& mp) : cta::common::dataStructures::MountPolicy(mp) {}

  void serialize (cta::objectstore::serializers::MountPolicy & osmp) const {
    osmp.set_name(name);
    osmp.set_archivepriority(archivePriority);
    osmp.set_archiveminrequestage(archiveMinRequestAge);
    osmp.set_retrievepriority(retrievePriority);
    osmp.set_retieveminrequestage(retrieveMinRequestAge);
    EntryLogSerDeser(creationLog).serialize(*osmp.mutable_creationlog());
    EntryLogSerDeser(lastModificationLog).serialize(*osmp.mutable_lastmodificationlog());
    osmp.set_comment(comment);
  }
  void deserialize (const cta::objectstore::serializers::MountPolicy & osmp) {
    name=osmp.name();
    archivePriority=osmp.archivepriority();
    archiveMinRequestAge=osmp.archiveminrequestage();
    retrievePriority=osmp.retrievepriority();
    retrieveMinRequestAge=osmp.retieveminrequestage();
    EntryLogSerDeser el;
    el.deserialize(osmp.creationlog());
    creationLog=el;
    el.deserialize(osmp.lastmodificationlog());
    lastModificationLog=el;
    comment=osmp.comment();
  }
};

} // namespace cta::objectstore
