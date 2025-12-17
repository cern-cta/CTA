/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <google/protobuf/service.h>

namespace cta::objectstore {

/**
 * This singleton class will check the compatibility of the runtime library
 * with the headers it was compiled against on construction (library load)
 * It will also cleanup
 */
class ProtobufCleaner {
  ProtobufCleaner() { GOOGLE_PROTOBUF_VERIFY_VERSION; }

  virtual ~ProtobufCleaner() {
    // Make protobuf cleanup not trigger memory leak detectors
    google::protobuf::ShutdownProtobufLibrary();
  }
} g_protobufCleaner;

}  // namespace cta::objectstore
