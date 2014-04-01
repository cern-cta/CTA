/******************************************************************************
 *                      DiskReadFileTask.hpp
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

#include "castor/tape/tapeserver/daemon/DiskReadTaskInterface.hpp"
#include "castor/tape/tapeserver/daemon/DataFifo.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
class DiskReadTask :public DiskReadTaskInterface {
public:
  DiskReadTask(DataConsumer & destination, int fileId, int nbBlocks): m_fileId(fileId),
      m_nbBlocks(nbBlocks), m_fifo(destination) {}
  /* Implementation of the DiskReadTask interface*/
  virtual bool endOfWork() { return false; }
  virtual void execute() {
    for (int blockId=0; blockId < m_nbBlocks; blockId++) {
      MemBlock * mb = m_fifo.getFreeBlock();
      mb->m_fileid = m_fileId;
      mb->m_fileBlock = blockId;
      m_fifo.pushDataBlock(mb);
    }
  }
private:
  int m_fileId;
  int m_nbBlocks;
  DataConsumer & m_fifo;
};

}}}}

