/******************************************************************************
 *                      TapeReadFileTask.hpp
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

#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/daemon/DataFifo.hpp"
#include "castor/tape/tapeserver/daemon/RecallMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"


namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
class TapeReadFileTask: public TapeReadTask {
public:
  TapeReadFileTask(RecallMemoryManager & source,DataConsumer & destination, int fSeq, int blockCount): m_fSeq(fSeq), 
          m_blockCount(blockCount), m_memManager(source),m_diskWriteTask(destination) {}
  /* Implementation of the TapeReadTask interface*/
  virtual bool endOfWork() { return false; }
  virtual void execute(castor::tape::drives::DriveInterface & td) {
    for (int blockId = 0; blockId < m_blockCount; blockId++) {
      MemBlock *mb = m_memManager.getFreeBlock();
      mb->m_fSeq = m_fSeq;
      mb->m_tapeFileBlock = blockId;
      //td.readBlock(mb);
      m_diskWriteTask.pushDataBlock(mb);
    }
    //printf("*** Done.\n");
  }
private:
  int m_fSeq;
  int m_blockCount;
  RecallMemoryManager & m_memManager;
  DataConsumer & m_diskWriteTask;
};
}
}
}
}
