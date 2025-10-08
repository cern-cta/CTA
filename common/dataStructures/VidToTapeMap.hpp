/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/dataStructures/Tape.hpp"

#include <map>
#include <string>

namespace cta::common::dataStructures {

/**
 * Map from tape volume identifier to tape.
 */
typedef std::map<std::string, Tape> VidToTapeMap;

} // namespace cta::common::dataStructures
