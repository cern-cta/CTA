/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include <common/dataStructures/MountType.hpp>
#include "common/dataStructures/LabelFormat.hpp"

namespace castor::tape::tapeserver::daemon {

/**
 * Struct holding the result of a Volume request
 */
struct VolumeInfo {
  std::string vid;                                           //!< The Volume ID (tape) we will work on
  cta::common::dataStructures::MountType mountType;          //!< Mount type: archive or retrieve
  uint32_t nbFiles;                                          //!< Number of files currently on tape
  std::string mountId;                                       //!< Mount ID
  cta::common::dataStructures::Label::Format labelFormat;    //!< Label/Tape format
  std::optional<std::string> encryptionKeyName;              //!< Encryption key ID
  std::string tapePool;                                      //!< Name of the tape pool that contains the tape
};

} // namespace castor::tape::tapeserver::daemon
