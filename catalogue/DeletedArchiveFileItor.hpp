/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include "common/dataStructures/DeletedArchiveFile.hpp"

namespace cta {
namespace catalogue {

class DeletedArchiveFileItorImpl;

class DeletedArchiveFileItor {
public:

  /**
   * Constructor.
   */
  DeletedArchiveFileItor();

  /**
   * Constructor.
   *
   * @param impl The object actually implementing this iterator.
   */
  DeletedArchiveFileItor(DeletedArchiveFileItorImpl *const impl);

  /**
   * Deletion of copy constructor.
   */
  DeletedArchiveFileItor(const DeletedArchiveFileItor &) = delete;

  /**
   * Move constructor.
   *
   * @param other The other object to be moved.
   */
  DeletedArchiveFileItor(DeletedArchiveFileItor &&other);

  /**
   * Destructor.
   */
  ~DeletedArchiveFileItor();

  /**
   * Deletion of copy assignment.
   */
  DeletedArchiveFileItor &operator=(const DeletedArchiveFileItor &) = delete;

  /**
   * Move assignment.
   */
  DeletedArchiveFileItor &operator=(DeletedArchiveFileItor &&rhs);

  /**
   * Returns true if a call to next would return another archive file.
   */
  bool hasMore() const;

  /**
   * Returns the next archive or throws an exception if there isn't one.
   */
  common::dataStructures::DeletedArchiveFile next();

private:

  /**
   * The object actually implementing this iterator.
   */
  DeletedArchiveFileItorImpl *m_impl;
};

}}
