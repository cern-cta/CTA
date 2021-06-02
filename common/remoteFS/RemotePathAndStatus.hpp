/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
  RemotePathAndStatus(
    const RemotePath &path,
    const RemoteFileStatus &status);

  /**
   * The path of the file in the remote storage system.
   */
  RemotePath path;

  /**
   * The status of the file in the remote storage system.
   */
  RemoteFileStatus status;

}; // class RemotePathAndStatus

} // namespace cta
