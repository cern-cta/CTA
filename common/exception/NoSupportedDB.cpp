/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/NoSupportedDB.hpp"

namespace cta::exception {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
NoSupportedDB::NoSupportedDB(const std::string &what):
  Exception(what) {
}

} // namespace cta::exception
