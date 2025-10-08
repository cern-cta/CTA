/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/dataStructures/OwnerIdentity.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * The status of a remote file or a directory.
 */
struct RemoteFileStatus {
  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  RemoteFileStatus();

  /**
   * Constructor.
   *
   * @param owner The identity of the owner.
   * @param mode The mode bits of the file or directory.
   * @param size The size of the file in bytes.
   */
  RemoteFileStatus(
    const common::dataStructures::OwnerIdentity &owner,
    const mode_t mode,
    const uint64_t size);

  /**
   * The identity of the owner.
   */
  common::dataStructures::OwnerIdentity owner;

  /**
   * The mode bits of the directory entry.
   */
  mode_t mode;

  /**
   * The size of the file in bytes.
   */
  uint64_t size;

}; // class RemoteFileStatus

} // namespace cta
