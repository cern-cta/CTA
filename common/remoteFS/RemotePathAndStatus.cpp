/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/remoteFS/RemotePathAndStatus.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemotePathAndStatus::RemotePathAndStatus(const RemotePath& path, const RemoteFileStatus& status)
    : path(path),
      status(status) {}
