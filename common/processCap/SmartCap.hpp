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

#include <stddef.h>
#include <sys/capability.h>

namespace cta { namespace server {

/**
 * A smart pointer that owns a capability state.
 *
 * This smart pointer will call cap_free() if it needs to free the owned
 * resource.
 *
 * Please note that process capabilities are not supported on Mac OS X.
 */
class SmartCap {
public:

  /**
   * Constructor.
   */
  SmartCap() throw();

  /**
   * Constructor.
   *
   * @param cap The capability state to be owned.
   */
  SmartCap(cap_t cap) throw();

  /**
   * Takes ownership of the specified capability state.  If this smart pointer
   * already owns a capbility state that is not the same as the one specified
   * then it will be freed using cap_free().
   *
   * @param cap The capability state to be owned.  If a capabibility state is
   * not specified then the default value of nullptr will be used.  In this default
   * case the smart pointer will not own a capbility state after the reset()
   * method returns.
   */
  void reset(cap_t cap = nullptr) throw();

  /**
   * SmartCap assignment operator.
   *
   * This function does the following:
   * <ul>
   * <li> Calls release on the previous owner (obj);
   * <li> Resets this smart pointer to the released pointer of the previous
   * owner (obj).
   * </ul>
   */
  SmartCap &operator=(SmartCap& obj);

  /**
   * Destructor.
   *
   * Resets this smart pointer with the default value of nullptr.
   */
  ~SmartCap() throw();

  /**
   * Returns the owned capbility state or nullptr if this smart pointer does not
   * own one.
   *
   * @return The owned capbility state or nullptr if this smart pointer does not
   * own one.
   */
  cap_t get() const throw();

  /**
   * Releases the owned capability state.
   *
   * @return The released capability state.
   */
  cap_t release();

private:

  /**
   * The owned capbility state or nullptr if this smart pointer does not own one.
   */
  cap_t m_cap;

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   *
   * Not implemented so that it cannot be called.
   */
  SmartCap(const SmartCap &obj) throw();

}; // class SmartCap

}} // namespace cta::server
