/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/wrapper/ConnWrapper.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
ConnWrapper::~ConnWrapper() = default;

} // namespace cta::rdbms::wrapper
