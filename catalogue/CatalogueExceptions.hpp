/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <common/exception/Exception.hpp>

namespace cta::exception {

CTA_GENERATE_EXCEPTION_CLASS(NegativeDiskSpaceReservationReached);
CTA_GENERATE_EXCEPTION_CLASS(CommentOrReasonWithMoreSizeThanMaximunAllowed);

} // namespace cta::exception
