/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "VirtualOrganization.hpp"
#include "common/dataStructures/EntryLog.hpp"

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct specifies the number of copies that a file tagged with it should
 * have. it may also indicate the VO owning the file and what kind of data the
 * file contains
 */
struct StorageClass {
  /**
   * Constructor that sets all integer member-variables to zero and all string
   * member-variables to the empty string.
   */
  StorageClass();

  /**
   * Returns true if the specified StorageClass object is considered equal to
   * this one.
   *
   * Please note that two StorageClass objects are considered equal if and only
   * if their name member-variables match, i.e.
   *
   *    name==rhs.name;
   *
   * All other member-variables are intentionally ignored by this equality
   * operator.
   *
   * @param rhs The other StorageClass object on the right-hand side of the
   * equality operator.
   * @return True if the specified StorageClass object is considered equal to
   * this one.
   */
  bool operator==(const StorageClass& rhs) const;

  /**
   * Returns the logical negation of operator==().
   *
   * @param rhs The other StorageClass object on the right-hand side of the
   * inequality operator.
   * @return The logical negation of operator==().
   */
  bool operator!=(const StorageClass& rhs) const;

  /**
   * The name of the storage class.
   *
   * Please note that the name of a storage class is only gauranteed to be
   * unique within its disk instance.
   */
  std::string name;

  /**
   * The number of copies on tape.
   */
  uint64_t nbCopies = 0;

  /**
   * The virtual organization to which this storage class belongs
   */
  VirtualOrganization vo;

  /**
   * The creation log.
   */
  EntryLog creationLog;

  /**
   * Th alst modification log.
   */
  EntryLog lastModificationLog;

  /**
   * The comment.
   */
  std::string comment;

};  // struct StorageClass

std::ostream& operator<<(std::ostream& os, const StorageClass& obj);

}  // namespace cta::common::dataStructures
