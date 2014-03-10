/******************************************************************************
 *                      TapeWriteTask.hpp
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

#include "castor/tape/tapeserver/daemon/Exception.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"

/**
 * Abstract class describing the interface for a task that wants to write to disk.
 * This is inherited exclusively by DiskWriteFileTask.
 */
class TapeWriteTask {
public:
  
  /**
   * TODO: see comment on the same function in DiskWriteFileTask.
   */
  virtual bool endOfWork() = 0;
  
  /**
   * Main routine of the task
   */
  virtual void execute(castor::tape::drives::DriveInterface & /*td*/) { 
    throw MemException("Tring to execute a non-execuatble TapeWriteTask"); 
  };
  
  /**
   * @return the number of files to write to disk
   */
  virtual int files() { return 0; }
  
  /**
   * @return the number of memory blocks to be used
   */
  virtual int blocks() { return 0; }
  
  /**
   * @return the file sequence number of the file to be written on tape
   */
  virtual int fSeq() { return -1; }
  
  /**
   * Destructor
   */
  virtual ~TapeWriteTask() {};
};
