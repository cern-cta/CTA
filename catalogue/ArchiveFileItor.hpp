/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/dataStructures/ArchiveFile.hpp"

namespace cta {
namespace catalogue {

/**
 * Forward declaration.
 */
class ArchiveFileItorImpl;

/**
 * Forward declaration.
 */
class Catalogue;

/**
 * A wrapper around an object that iterators over a list of archive files.
 *
 * This wrapper permits the user of the Catalogue API to use different
 * iterator implementations whilst only using a single iterator type.
 */
class ArchiveFileItor {
public:

  /**
   * Constructor.
   */
  ArchiveFileItor();

  /**
   * Constructor.
   *
   * @param impl The object actually implementing this iterator.
   */
  ArchiveFileItor(ArchiveFileItorImpl *const impl);

  /**
   * Deletion of copy constructor.
   */
  ArchiveFileItor(const ArchiveFileItor &) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object to be moved.
   */
  ArchiveFileItor(ArchiveFileItor &&other);

  /**
   * Destructor.
   */
  ~ArchiveFileItor();

  /**
   * Deletion of copy assignment.
   */
  ArchiveFileItor &operator=(const ArchiveFileItor &) = delete;

  /**
   * Move assignment.
   */
  ArchiveFileItor &operator=(ArchiveFileItor &&rhs);

  /**
   * Returns true if a call to next would return another archive file.
   */
  bool hasMore() const;

  /**
   * Returns the next archive or throws an exception if there isn't one.
   */
  common::dataStructures::ArchiveFile next();

private:

  /**
   * The object actually implementing this iterator.
   */
  ArchiveFileItorImpl *m_impl;

}; // class ArchiveFileItor

} // namespace catalogue
} // namespace cta
