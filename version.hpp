/*
 * SPDX-FileCopyrightText: 2015 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <array>
#include <cstdint>

// External declarations for values defined in version.cpp
extern const char CTA_VERSION[];
extern const int CTA_CATALOGUE_SCHEMA_VERSION_MAJOR;
extern const int CTA_CATALOGUE_SCHEMA_VERSION_MINOR;

extern const int SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_ARRAY[];
extern const std::size_t SUPPORTED_CTA_CATALOGUE_SCHEMA_VERSIONS_COUNT;

extern const char XROOTD_SSI_PROTOBUF_INTERFACE_VERSION[];
extern const char LOG_SCHEMA_VERSION[];
