/******************************************************************************
 *                      BlockKey.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: BlockKey.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/11/03 15:34:38 $ $Author: sponcec3 $
 *
 * The identification of a shared memory block
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_BLOCKKEY_HPP
#define SHAREDMEMORY_BLOCKKEY_HPP 1

#include <memory>

namespace castor {

  namespace sharedMemory {

    /**
     * The identification of a shared memory block,
     * that is the key, the size and the address where
     * it should be mapped in case we crea
     */
    class BlockKey {

    public:

      /**
       * constructor
       */
      BlockKey(key_t key, size_t size, const void* address) throw();

      /**
       * Get accessor to member m_key
       * @return the current value of m_key
       */
      key_t key () const {
        return m_key;
      }

      /**
       * Get accessor to member m_size
       * @return the current value of m_size
       */
      size_t size () const {
        return m_size;
      }

      /**
       * Get accessor to member m_address
       * @return the current value of m_address
       */
      const void* address () const {
        return m_address;
      }

      /**
       * operator < so that it can be used as map index
       */
      bool operator<(const BlockKey k) const {
        return key() < k.key();
      }

    private:

      ///--- Move this line to the private section...
      const void* m_address;

      ///--- Move this line to the private section...
      size_t m_size;

      ///--- Move this line to the private section...
      key_t m_key;

    }; // class BlockKey

  } // namespace sharedMemory

} // namespace castor

#endif // SHAREDMEMORY_BLOCKKEY_HPP
