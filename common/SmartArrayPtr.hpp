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

#include "common/exception/NotAnOwner.hpp"

#include <errno.h>
#include <stdio.h>

namespace cta {

/**
 * A smart pointer that owns a pointer to an array and unlike std::unique_ptr
 * will call delete[] instead of calling delete.
 */
template<typename T>
class SmartArrayPtr {
public:
  /**
   * Constructor.
   */
  SmartArrayPtr() throw() : m_arrayPtr(nullptr) {}

  /**
   * Constructor.
   *
   * @param arrayPtr The pointer to an array that is to be owned by the smart
   * pointer.
   */
  SmartArrayPtr(T* const arrayPtr) throw() : m_arrayPtr(arrayPtr) {}

  /**
   * Takes ownership of the specified pointer to an array.  If this smart
   * pointer already owns a pointer that is not the same as the one specified
   * then it will be deleted using delete[].
   *
   * @param arrayPtr The pointer to be owned.  If no pointer is specified then
   * the default value of nullptr is used.  In this default case the smart pointer
   * will not own a pointer after the reset() method returns.
   */
  void reset(T* const arrayPtr = nullptr) throw() {
    // If the new pointer is not the one already owned
    if (arrayPtr != m_arrayPtr) {
      // If this smart pointer still owns a pointer then call delete[] on it
      if (m_arrayPtr != nullptr) {
        delete[] m_arrayPtr;
      }

      // Take ownership of the new pointer
      m_arrayPtr = arrayPtr;
    }
  }

  /**
   * SmartArrayPtr assignment operator.
   *
   * This function does the following:
   * <ul>
   * <li> Calls release on the previous owner (obj);
   * <li> Resets this smart pointer to the released pointer of the previous
   * owner (obj).
   * </ul>
   */
  SmartArrayPtr& operator=(SmartArrayPtr& obj) {
    reset(obj.release());
    return *this;
  }

  /**
   * Destructor.
   *
   * Resets this smart pointer with the default value of nullptr.
   */
  ~SmartArrayPtr() throw() { reset(); }

  /**
   * Returns the owned pointer or nullptr if this smart pointer does not own one.
   *
   * @return The owned pointer or nullptr if this smart pointer does not own one.
   */
  T* get() const throw() { return m_arrayPtr; }

  /**
   * Releases the owned pointer.
   *
   * @return The released pointer.
   */
  T* release() {
    // If this smart pointer does not own a pointer
    if (nullptr == m_arrayPtr) {
      cta::exception::NotAnOwner ex;
      ex.getMessage() << "Smart pointer does not own a pointer";
      throw(ex);
    }

    // Assigning nullptr to m_arrayPtr indicates this smart pointer does not own a
    // pointer
    T* const tmpArrayPtr = m_arrayPtr;
    m_arrayPtr = nullptr;
    return tmpArrayPtr;
  }

  /**
   * Subscript operator.
   */
  T& operator[](const int i) const throw() { return m_arrayPtr[i]; }

private:
  /**
   * The owned pointer.  A value of nullptr means this smart pointer does not own
   * a pointer.
   */
  T* m_arrayPtr;

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   *
   * Not implemented so that it cannot be called
   */
  SmartArrayPtr(const SmartArrayPtr& obj) throw();

};  // class SmartArrayPtr

}  // namespace cta
