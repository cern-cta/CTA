/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/remoteFS/RemoteFileStatus.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemoteFileStatus::RemoteFileStatus():
  mode(0),
  size(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemoteFileStatus::RemoteFileStatus(
  const common::dataStructures::OwnerIdentity &owner,
  const mode_t mode,
  const uint64_t size):
  owner(owner),
  mode(mode),
  size(size) {
}
