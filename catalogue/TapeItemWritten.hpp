/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdint.h>
#include <string>

namespace cta::catalogue {

/**
 * Structure describing the event of having written a file to tape.
 */
struct TapeItemWritten {

  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero.
   */
  TapeItemWritten() = default;

  /**
   * Default virtual destructor to make the object polymorphic
   */
  virtual ~TapeItemWritten() = default;

  /**
   * Equality operator.
   *
   * @param rhs The right hand side of the operator.
   */
  bool operator==(const TapeItemWritten &rhs) const;

  /**
   * Less than operator.
   *
   * TapeItemWritten events are ordered by their tape file sequence number.
   *
   * TapeItemWritten events are written to the catalogue database in batches in
   * order to improve performance by reducing the number of network round trips
   * to the database.  Each batch is ordered by tape file sequence number so
   * that the CTA catalogue code can easily assert that files that are written
   * to tape are reported correctly.
   *
   * @param rhs The right hand side of the operator.
   */
  bool operator<(const TapeItemWritten &rhs) const;

  /**
   * The volume identifier of the tape on which the file has been written.
   */
  std::string vid;

  /**
   * The position of the item on tape in the form of its file sequence number.
   */
  uint64_t fSeq = 0;

  /**
   * The name of the tape drive that wrote the item.
   */
  std::string tapeDrive;

}; // struct TapeFileWritten

/**
 * Output stream operator for an TapeItemWritten object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream &operator<<(std::ostream &os, const TapeItemWritten &obj);

} // namespace cta::catalogue
