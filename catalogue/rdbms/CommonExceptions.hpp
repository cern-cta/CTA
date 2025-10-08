/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <common/exception/Exception.hpp>
#include <common/exception/UserError.hpp>

namespace cta::catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringComment);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroRefreshInterval);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringFreeSpaceQueryURL);

} // namespace cta::catalogue
