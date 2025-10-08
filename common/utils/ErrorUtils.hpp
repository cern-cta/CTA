/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#include <string>

namespace cta::utils {

/**
 * Determines the string representation of the specified error number.
 *
 * Please note this method is thread safe.
 *
 * @param errnoValue The errno value.
 * @return The string representation.
 */
  std::string errnoToString(const int errnoValue);
} // namespace cta::utils
