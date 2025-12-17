/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/remoteFS/RemoteFileStatus.hpp"
#include "common/remoteFS/RemotePath.hpp"

namespace cta {

/**
 * The path and status of a file in a remote storage system.
 */
struct RemotePathAndStatus {
  /**
   * Constructor
   */
  RemotePathAndStatus() = default;

  /**
   * Constructor
   *
   * @param path The path of the file in the remote storage system.
   * @param status The status of the file in the remote storage system.
   */
  RemotePathAndStatus(const RemotePath& path, const RemoteFileStatus& status);

  /**
   * The path of the file in the remote storage system
   */
  RemotePath path;

  /**
   * The status of the file in the remote storage system
   */
  RemoteFileStatus status;
};

}  // namespace cta
