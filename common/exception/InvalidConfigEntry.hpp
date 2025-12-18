/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

#include <string>

namespace cta::exception {

/**
 * Invalid configuration entry exception.
 */
class InvalidConfigEntry : public Exception {
public:
  /**
   * Constructor
   *
   * @param catagory   The category of the configuration entry.
   * @param entryName  The name of the configuration entry.
   * @param entryValue The (invalid) value of the configuration entry.
   */
  InvalidConfigEntry(const char* const category, const char* const entryName, const char* const entryValue);

  /**
   * Trivial destructor (required through inheritence from std::exception)
   */
  ~InvalidConfigEntry() final = default;

  /**
   * Returns the category of the configuration entry.
   */
  const std::string& getEntryCategory();

  /**
   * Returns the name of the configuration entry.
   */
  const std::string& getEntryName();

  /**
   * Returns the (invalid) value of the configuration entry.
   */
  const std::string& getEntryValue();

private:
  /**
   * The category of the configuration entry.
   */
  const std::string m_entryCategory;

  /**
   * The name of the configuration entry.
   */
  const std::string m_entryName;

  /**
   * The (invalid) value of the configuration entry.
   */
  const std::string m_entryValue;
};

}  // namespace cta::exception
