/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <chrono>

namespace cta::utils {

enum class ReadTapeTestMode {
  USE_FSEC = 0,
  USE_BLOCK_ID_DEFAULT,
  USE_BLOCK_ID_1,
  USE_BLOCK_ID_2,
};

}  // namespace cta::utils
