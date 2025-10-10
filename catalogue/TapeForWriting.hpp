/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <optional>
#include "common/dataStructures/LabelFormat.hpp"
#include <ostream>
#include <stdint.h>
#include <string>

namespace cta::catalogue {

/**
 * A tape that can be written to.
 */
struct TapeForWriting {

  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero.
   *
   * Sets the value of all boolean member-variables to false.
   */
  TapeForWriting() = default;

  /**
   * Equality operator.
   *
   * Two tapes for writing are equal if and only if their volume identifiers
   * (VIDs) are equal.
   *
   * @param ths The right hand side of the operator.
   */
  bool operator==(const TapeForWriting &rhs) const;

  /**
   * The volume identifier of the tape.
   */
  std::string vid;

  /**
   * The media type of the tape.
   */
  std::string mediaType;

  /**
   * The vendor of the tape.
   */
  std::string vendor;

  /**
   * The name of the tape pool.
   */
  std::string tapePool;

  /**
   * The virtual organisation owning the tape.
   */
  std::string vo;

  /**
   * The file sequence number of the last file successfully written to the tape.
   */
  uint64_t lastFSeq = 0;

  /**
   * The capacity of the tape in bytes.
   */
  uint64_t capacityInBytes = 0;

  /**
   * The total amount of data written to the tape in bytes.
   */
  uint64_t dataOnTapeInBytes = 0;

  /**
   * The format type of the tape.
   */
  cta::common::dataStructures::Label::Format labelFormat;

  /**
   * Encryption key name to pass to external encryption script
   */
  std::optional<std::string> encryptionKeyName;

}; // struct TapeForWriting

/**
 * Output stream operator for an TapeForWriting object.
 *
 * This function writes a human readable form of the specified object to the
 * specified output stream.
 *
 * @param os The output stream.
 * @param obj The object.
 */
std::ostream &operator<<(std::ostream &os, const TapeForWriting &obj);

} // namespace cta::catalogue
