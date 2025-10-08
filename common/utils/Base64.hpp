/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta::utils {

/**
 * Decodes a base64 encoded input string.
 * @param input The encoded base64 string.
 * @return The decoded string.
 */
std::string base64decode(const std::string& input);

/**
 * Base64 encodes the provided input string.
 * @param input The string that should be base64 encoded.
 * @return The base64 encoded string.
 */
std::string base64encode(const std::string& input);

}  // namespace cta::utils
