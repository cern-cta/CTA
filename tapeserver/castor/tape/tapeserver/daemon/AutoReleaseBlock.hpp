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
#include <memory>
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

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
  
}}}}

