/******************************************************************************
 *                      Payload.hpp
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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

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

