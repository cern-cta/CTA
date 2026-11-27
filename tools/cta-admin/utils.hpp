/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::frontend::grpc::utils {

/*
 * Validate the format of a JWT
 */
bool isJwtFormatValid(const std::string& token);

}  // namespace cta::frontend::grpc::utils
