/******************************************************************************
 *                      RepackServerReqSvcThread.hpp
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
 * @(#)$RCSfile: RepackWorker.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2006/02/14 15:26:26 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/

#ifndef REPACKWORKER_HPP
#define REPACKWORKER_HPP 1

#include "castor/repack/RepackCommonHeader.hpp" /* the Common Header */
#include <sys/types.h>
#include <time.h>
#include <common.h>     /* for conversion of numbers to char */
#include <vector>
#include <map>



/* ============= */
/* Local headers */
/* ============= */
#include "vmgr_api.h"
#include "castor/IObject.hpp"
#include "castor/server/IThread.hpp"
#include "castor/MessageAck.hpp"
#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"
#include "FileListHelper.hpp"
#include "DatabaseHelper.hpp"



namespace castor {

 namespace repack {

  /**
   * A thread to process Put/Get request and submit them to the stager.
   */
  class RepackWorker : public castor::server::IThread {

  public:

    /**
     * Initializes the db access.
     */
    RepackWorker();
    
    /**
     * Standard destructor
     */
    ~RepackWorker() throw();


    
    virtual void run(void* param) throw();
    virtual void stop() throw();
    
  private:
    
   	/**
	 * Retrieves the Information of a tape and returns its status, otherwise -1
	 * @param vid The Volumeid of the tape
	 */
    int getTapeInfo(std::string tapename);

	bool checkTapeForRepack(std::string);

    /**
     * Retrieves information about the tapes in a pool and counts the tape
     * in the pool.
     * @param pool the name of the tape pool
     * @throws castor::exception::Exception if the pool does not exist
     */ 
    int getPoolInfo(castor::repack::RepackRequest* rreq) throw();

    /**
     * the nameserver, which is contacted for all Requests
     */
    char *m_nameserver;
    
    /**
     * the FileListHelper, which helps to get the filelist from the nameserver
     * of a tape.
     */
    castor::repack::FileListHelper* m_filelisthelper;

    /**
     * the DatabaseHelper, which helps to store the Request in the DB.
     */
    castor::repack::DatabaseHelper* m_databasehelper;
    
  };

 } // end of namespace repack

} // end of namespace castor


#endif // REPACKWORKER_HPP
