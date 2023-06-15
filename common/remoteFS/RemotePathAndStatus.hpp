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

#include "common/remoteFS/RemoteFileStatus.hpp"
#include "common/remoteFS/RemotePath.hpp"

namespace cta {

/**
 * The path and status of a file in a remote storage system.
 */
struct RemotePathAndStatus {
  /**
   * Constructor.
   */
  RemotePathAndStatus();

  /**
   * Constructor.
   *
   * @param path The path of the file in the remote storage system.
   * @param status The status of the file in the remote storage system.
   */
  RemotePathAndStatus(const RemotePath& path, const RemoteFileStatus& status);

  /**
   * The path of the file in the remote storage system.
   */
  RemotePath path;

  /**
   * The status of the file in the remote storage system.
   */
  RemoteFileStatus status;

};  // class RemotePathAndStatus

}  // namespace cta
