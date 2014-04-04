/******************************************************************************
 *                      DiskWriteTask.hpp
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




namespace castor {
  namespace log{
  class LogContext;
  }
namespace tape {
namespace tapeserver {
namespace daemon {
class RecallReportPacker;
/**
 * Abstract class describing the interface for a task that wants to write to disk.
 * This is inherited exclusively by DiskWriteFileTask.
 */
class DiskWriteTaskInterface {
public:
  
  /**
   * @return the number of memory blocks to be used
   */
  virtual int blocks() =0;
  
  /**
   * @return the number of files to write to disk
   */
  virtual int files() =0;
  
  /**
   * Main routine of the task
   */
  virtual void execute(RecallReportPacker& reporter,log::LogContext& lc) =0;
  
  /**
   * Wait for the end of the task
   */
  virtual void waitCompletion() {};
  
  /**
   * Destructor
   */
  virtual ~DiskWriteTaskInterface() {};
};

}}}}
