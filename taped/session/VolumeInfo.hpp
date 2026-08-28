/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/LabelFormat.hpp"

#include <common/dataStructures/MountType.hpp>
#include <string>

namespace cta::tape::daemon {

/// Tape and mount metadata captured from the scheduler assignment.
struct VolumeInfo {
  std::string vid;
  cta::common::dataStructures::MountType mountType;
  uint32_t nbFiles;  ///< Number of files already on tape before this assignment.
  std::string mountId;
  cta::common::dataStructures::Label::Format labelFormat;
  std::optional<std::string> encryptionKeyName;
  std::string tapePool;
};

}  // namespace cta::tape::daemon
