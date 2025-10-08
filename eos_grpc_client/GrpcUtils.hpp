/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace eos::client {

/*!
 * Basename and pathname of a path
 */
struct Dirname {
  std::string basename;                                //!< Just the directory name
  std::string pathname;                                //!< The full path including the directory name
};

/*!
 * Enforce prefixes to begin and end with a slash
 */
void checkPrefix(std::string &prefix);

/*!
 * Remove the first prefix from pathname, prepend the second prefix, then split path into basename and pathname parts
 *
 * Note: prefixes must begin and end with a slash
 */
Dirname manglePathname(const std::string &remove_prefix, const std::string &add_prefix, const std::string &pathname, const std::string &filename = "");

} // namespace eos::client
