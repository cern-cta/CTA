/*
 * SPDX-FileCopyrightText: 2015 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <set>

// External declarations for values defined in version.cpp
extern const char CTA_VERSION[];
extern const int CTA_CATALOGUE_SCHEMA_VERSION_MAJOR;
extern const int CTA_CATALOGUE_SCHEMA_VERSION_MINOR;

extern const std::set<int> SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS;
extern const std::size_t SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_COUNT;

extern const char XROOTD_SSI_PROTOBUF_INTERFACE_VERSION[];
extern const char LOG_SCHEMA_VERSION[];
