/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
