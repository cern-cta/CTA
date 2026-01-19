/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "version.hpp"

// Populated by CMake configure_file
const char CTA_VERSION[] = "0-1";

const int CTA_CATALOGUE_SCHEMA_VERSION_MAJOR = 15;
const int CTA_CATALOGUE_SCHEMA_VERSION_MINOR = 0;

const std::set<int> SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS = {
    15
};
const std::size_t SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_COUNT = 1;

const char XROOTD_SSI_PROTOBUF_INTERFACE_VERSION[] = "v0.0";

const char LOG_SCHEMA_VERSION[] = "0.1.0";
