/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

// At the moment only used for meter names.
// However, if these values are used in other places in the future, we can make this more generic.
namespace cta::semconv::meter {

static constexpr const char* kCtaFrontend = "cta.frontend";
static constexpr const char* kCtaRdbms = "cta.rdbms";
static constexpr const char* kCtaScheduler = "cta.scheduler";
static constexpr const char* kCtaObjectstore = "cta.objectstore";
static constexpr const char* kCtaTaped = "cta.taped";

}  // namespace cta::semconv::meter
