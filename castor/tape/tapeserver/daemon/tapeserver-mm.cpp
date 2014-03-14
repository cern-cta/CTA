/******************************************************************************
 *                      tapeserver-mm.cpp
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

#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteFileTask.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadFileTask.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteFileTask.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadFileTask.hpp"
#include "castor/tape/tapeserver/daemon/MockTapeGateway.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include <memory>

const size_t blocks = 100;
const size_t blockSize = 5242880;
const int filesNumber = 200;
const int diskThreads = 10;
const int modulo = 11;
const int maxFilesReq = 5;
const int maxBlocksReq = 100;
const int maxFilesBeforeFlush = 5;
const int maxBlocksBeforeFlush = 29;
int idToSize(int id) {
    return (id % modulo + 1);
}

int main(void) {

  //try {
  //  while (1) new char[100000];
  //} catch (std::exception e) {
  //  printf("%s\n", e.what());
  //}
  // Allocate the memory
  MemoryManager mm(blocks, blockSize);
  mm.startThreads();
  castor::tape::drives::FakeDrive drive;
  {
    MockTapeGateway tg(filesNumber, modulo);
    MigrationReportPacker mrp(tg);
    mrp.startThreads();
    TapeWriteSingleThread tapeThread(drive, mrp, maxFilesBeforeFlush, maxBlocksBeforeFlush);
    DiskReadThreadPool diskThreadPool(diskThreads);
    tapeThread.startThreads();
    diskThreadPool.startThreads();
    for (int i = 0; i < filesNumber; i++) {
      int size = idToSize(i);
      TapeWriteFileTask * twt = new TapeWriteFileTask(i, size, mm);
      DiskReadFileTask * dwt = new DiskReadFileTask(*twt, i + 1000, size);
      tapeThread.push(twt);
      diskThreadPool.push(dwt);
    }
    diskThreadPool.finish();
    tapeThread.finish();
    diskThreadPool.waitThreads();
    printf("disk read threads complete\n");
    tapeThread.waitThreads();
    printf("tape write thread complete\n");
    mrp.waitThread();
    printf("tapeGateway reporter complete\n");
  }
  printf("========= Switching to recall session\n");
  /* recall session */
  /*
  {
    MockTapeGateway tg(filesNumber, modulo); 
    TapeReadSingleThread trtp(drive);
    DiskWriteThreadPool dwtp(diskThreads, maxFilesReq, maxBlocksReq);
    RecallJobInjector rji(mm, trtp, dwtp, tg);
    dwtp.setJobInjector(&rji);
    trtp.startThreads();
    dwtp.startThreads();
    rji.startThreads();
    // Give the initial kick to the session (it's a last call) 
    rji.requestInjection(maxFilesReq, maxFilesReq, true);
    trtp.waitThreads();
    printf("tape read thread complete\n");
    dwtp.waitThreads();
    printf("disk write threads complete\n");
    rji.waitThreads();
    printf("Recall job injector complete\n");
  }*/
  mm.finish();
  mm.waitThreads();
  printf("memory management thread complete\n");
}
