/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta::frontend::grpc::utils {

/**
 * Load the content of the file into a string
 */
void read(const std::string& strPath, std::string& strValu);

} // namespace cta::frontend::grpc::utils
