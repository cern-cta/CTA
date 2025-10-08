/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once
#include <memory>
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"

namespace castor::tape::tapeserver::daemon {

/*
 * Use RAII to make sure the memory block is released
 *(ie pushed back to the memory manager) in any case (exception or not)
 * Example of use
 * {
 *   MemBlock* block = getItFromSomewhere()
 *   {Recall/Migration}MemoryManager mm;
 *   AutoReleaseBlock releaser(block,mm);
 * }
 */
 template <class MemManagerT> class AutoReleaseBlock {
   /**
    * The block to release
    */
   MemBlock* const m_block;

   /**
    * To whom it should be given back
    */
   MemManagerT& memManager;
  public:
    /**
     *
     * @param mb he block to release
     * @param mm To whom it should be given back
     */
    AutoReleaseBlock(MemBlock* const mb,MemManagerT& mm):
    m_block(mb),memManager(mm){}

    //let the magic begin
    ~AutoReleaseBlock(){
      memManager.releaseBlock(m_block);
    }
  };

} // namespace castor::tape::tapeserver::daemon

