/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "mediachanger/LibrarySlot.hpp"

namespace cta::mediachanger {

/**
 * Class representing a dummy slot for the tests.
 */
class DummyLibrarySlot: public LibrarySlot {
public:
  /**
   * Constructor
   *
   * Sets all string member-variables to the empty string.
   */
  DummyLibrarySlot();

  /**
   * Constructor
   *
   * Initialises the member variables based on the result of parsing the
   * specified string representation of the library slot.
   *
   * @param str The string representation of the library slot.
   */
  explicit DummyLibrarySlot(const std::string& str);

  /**
   * Destructor
   */
  ~DummyLibrarySlot() = default;

  /**
   * Creates a clone of this object.
   *
   * @return The clone.
   */
  LibrarySlot *clone();
};

} // namespace cta::mediachanger
