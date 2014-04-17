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
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Abstract class describing the interface for a task that wants to write to disk.
 * This is inherited exclusively by DiskWriteFileTask.
 */
class TapeWriteTaskInterface {
public:
  
  /**
   * Main routine of the task
   */
  virtual void execute(castor::tape::tapeFile::WriteSession&,
          MigrationReportPacker & reportPacker,castor::log::LogContext& lc) =0;
    
  /**
   * @return the number of memory blocks to be used
   */
  virtual int blocks() =0; 
  
  /**
   * Destructor
   */
  virtual ~TapeWriteTaskInterface() {};
};
}
}
}
}
