/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::rdbms {

/**
 * A database statement can either have auto commiting mode turned on or off.
 */
enum class AutocommitMode {
  AUTOCOMMIT_ON,
  AUTOCOMMIT_OFF
};

} // namespace cta::rdbms
