/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/utils/ErrorUtils.hpp"

#include <sstream>
#include <stdint.h>
#include <string>
#include <vector>

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
uint8_t toUint8(std::string_view str);

/**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unisgned integer.
   */
uint16_t toUint16(std::string_view str);

/**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unsigned integer.
   */
uint32_t toUint32(std::string_view str);

/**
   * Parses the specified string representation of an unsigned 64-bit integer.
   *
   * Please note that "-1" is a valid string and will parse successfully.
   *
   * @return The parsed unsigned 64-bit integer.
   */
uint64_t toUint64(std::string_view str);

/**
   * Parses the specified string representation of a double.
   *
   * @return The parsed double.
   */
double toDouble(std::string_view str);

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
bool isValidUInt(std::string_view str);

/**
 * Checks if the specified string is a valid decimal.
 *
 * @param str The string to be checked.
 * @returns true if the string is a valid decimal, else false.
 */
bool isValidDecimal(std::string_view str);

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

/**
 * @brief Splits a comma-separated string into a vector of strings.
 * @param input The comma-separated string.
 * @param separator by default a comma ","
 * @return A vector of strings.
 */
std::vector<std::string> splitStringToVector(const std::string& input, const char separator = ',');

/**
 * @brief Selects the next VID from alternateStrings relative to currentString.
 * @param currentString The current VID.
 * @param alternateStrings A comma-separated list of strings, e.g. alternate VIDs.
 * @return The next string and index of it or currentString, 0 if no valid next string is found .
 */
std::pair<std::string, size_t> selectNextString(const std::string& currentString, const std::string& alternateStrings);

}  // namespace cta::utils
