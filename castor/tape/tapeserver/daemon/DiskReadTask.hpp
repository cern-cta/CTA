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
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
class DiskReadTask :public DiskReadTaskInterface {
public:
  /**
   * @param destination The task that will consume data block we fill up
   * @param file the file we are migrating. We acquire the ownership of the pointer
   * @param numberOfBlock number of memory block we need read the whole file
   */
  DiskReadTask(DataConsumer & destination, 
          tape::tapegateway::FileToMigrateStruct* file,size_t numberOfBlock);
  
  virtual void execute(log::LogContext& lc);
private:
  /**
   * The task (a TapeWriteTask) that will handle the read blocks
   */
  DataConsumer & m_nextTask;
  
  /**
   * All we need to know about the file we are migrating
   */
  std::auto_ptr<tape::tapegateway::FileToMigrateStruct> m_migratedFile;
  
  /**
   * The number of memory block we will need to read the whole file
   */
  size_t m_numberOfBlock;
};

}}}}

