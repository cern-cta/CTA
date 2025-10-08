/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <map>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * Map from tape copy number to destination tape pool name.
 */
typedef std::map<uint32_t, std::string> TapeCopyToPoolMap;

} // namespace cta::common::dataStructures
