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
  TapeItemWritten();
  
  /**
   * Virtual trivial destructor to make the object polymorphic.
   */
  virtual ~TapeItemWritten() {}

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
  uint64_t fSeq;
  
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
