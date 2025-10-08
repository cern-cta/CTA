/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "version.h"

#pragma once

namespace cta {

const int CA_MAXVIDLEN = 6; // maximum length for a VID
const int CTA_SCHEMA_VERSION_MAJOR = CTA_CATALOGUE_SCHEMA_VERSION_MAJOR;
const int CTA_SCHEMA_VERSION_MINOR = CTA_CATALOGUE_SCHEMA_VERSION_MINOR;
const int TAPE_LABEL_UNITREADY_TIMEOUT = 300;
const std::string SCHEDULER_NAME_CONFIG_KEY = "SchedulerBackendName";
} // namespace cta


