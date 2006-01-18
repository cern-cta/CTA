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
 * @(#)$RCSfile: RepackWorker.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/01/18 14:17:33 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/

#ifndef REPACKWORKER_HPP
#define REPACKWORKER_HPP 1

#include "castor/repack/RepackCommonHeader.hpp"
#include "castor/IObject.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/server/IThread.hpp"
#include "castor/MessageAck.hpp"
#include "RepackRequest.hpp"
#include "FileListHelper.hpp"
#include "DatabaseHelper.hpp"

#define BUFSIZE 1024


namespace castor {

 namespace repack {

  static int counter;

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

    /**
     * Initialization for this thread
     * XXX @param is the ThreadPool owning the thread,
     * we ignore it for the time being.
     */
    virtual void init(void* param);
    
    
    virtual void run() throw();
    virtual void stop() throw();
    
  private:
    void send_Ack(MessageAck ack, castor::io::ServerSocket* sock);
    int getTapeInfo(const std::string vid);
    void* m_param;
    char *m_nameserver;
    castor::repack::FileListHelper* m_filelisthelper;
    castor::repack::DatabaseHelper* m_databasehelper;
    
  };

 } // end of namespace repack

} // end of namespace castor


#endif // REPACKWORKER_HPP
