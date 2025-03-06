/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "common/utils/ErrorUtils.hpp"

#include <sstream>
#include <stdint.h>
#include <string>

namespace cta::utils {

/**
 * Returns the string reprsentation of the specified value.
 *
 * @param value The value whose string representation is to be returned.
 * @return The string reprsentation of the specified value.
 */
template<typename T>
std::string toString(const T& value) {
  std::ostringstream oss;
  oss << value;
  return oss.str();
}

/**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unisgned integer.
   */
uint8_t toPGUint8(std::string_view str);

/**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unisgned integer.
   */
uint16_t toPGUint16(std::string_view str);

/**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unsigned integer.
   */
uint32_t toPGUint32(std::string_view str);

/**
   * Parses the specified string representation of an unsigned 64-bit integer.
   *
   * Please note that "-1" is a valid string and will parse successfully.
   *
   * @return The parsed unsigned 64-bit integer.
   */
uint64_t toPGUint64(std::string_view str);

/**
   * Parses the specified string representation of a double.
   *
   * @return The parsed double.
   */
double toPGDouble(std::string_view str);

/**
   * Parses the specified string representation of an unsigned 64-bit integer.
   *
   * Please note that "-1" is a valid string and will parse successfully.
   *
   * @return The parsed unsigned 64-bit integer.
   */
uint64_t toUint64(const std::string& str);

/**
 * Converts the specified string to an unsigned integer.
 *
 * @param str The string.
 * @return The unisgned integer.
 */
uint8_t toUint8(const std::string& str);

/**
 * Converts the specified string to an unsigned integer.
 *
 * @param str The string.
 * @return The unisgned integer.
 */
uint16_t toUint16(const std::string& str);

/**
 * Converts the specified string to an unsigned integer.
 *
 * @param str The string.
 * @return The unsigned integer.
 */
uint32_t toUint32(const std::string& str);

/**
 * Converts the specified string to a uid.
 *
 * @param str The string.
 * @return The uid.
 */
uid_t toUid(const std::string& str);

/**
 * Converts the specified string to a gid.
 *
 * @param str The string.
 * @return The gid.
 */
gid_t toGid(const std::string& str);

/**
 * Checks if the specified string is a valid unsigned integer.
 *
 * @param str The string to be checked.
 * @returns true if the string is a valid unsigned integer, else false.
 */
bool isValidUInt(const std::string& str);

/**
 * Parses the specified string representation of an unsigned 64-bit integer.
 *
 * Please note that "-1" is a valid string and will parse successfully.
 *
 * @return The parsed unsigned 64-bit integer.
 */
uint64_t toUint64(const std::string& str);

/**
 * Checks if the specified string is a valid decimal.
 *
 * @param str The string to be checked.
 * @returns true if the string is a valid decimal, else false.
 */
bool isValidDecimal(const std::string& str);

/**
 * Parses the specified string representation of a double.
 *
 * @return The parsed double.
 */
double toDouble(const std::string& str);

/**
 * Converts the specified string to uppercase.
 *
 * @param In/out parameter: The string to be converted.
 */
void toUpper(std::string& str);

/**
 * Checks if the specified string is uppercase.
 *
 *  @param In/out parameter: The string to be checked.
 *  @return true if the string is uppercase.
 */
bool isUpper(const std::string& str);

/**
 * Converts the specified string to lowercase.
 *
 * @param In/out parameter: The string to be converted.
 */
void toLower(std::string& str);

}  // namespace cta::utils
