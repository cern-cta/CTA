/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

namespace eos {
namespace client {

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

}} // namespace eos::client
