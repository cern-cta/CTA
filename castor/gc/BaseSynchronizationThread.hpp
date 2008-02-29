/******************************************************************************
 *                      BaseSynchronizationThread.hpp
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
 * @(#)$RCSfile: BaseSynchronizationThread.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/02/29 11:01:56 $ $Author: mmartins $
 *
 * Base Thread Class for the Threads going through the files stored on the CASTOR related
 * filesystem and checking their existence either in the NameServer (NameServerSynchronizationThread)
 * or in the Stager (StagerSynchronizationThread)
 * In case they were dropped, either the Stager (NameServerSynchronizationThread)
 *  or the NameServer (StagerSynchronizationThread) is informed and drops the file
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef GC_GCDAEMON_BASE_SYNCHRONIZATION_THREAD_HPP
#define GC_GCDAEMON_BASE_SYNCHRONIZATION_THREAD_HPP 1

// Include files
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"
#include <string>
#include <vector>


/**********************/
/* related constants */
/********************/

/* SynchronizationThread type */
enum syncThreadType {Sync_NameServer, Sync_Stager}

namespace castor {

  namespace gc {

    /**
     * BaseSynchronization Thread
     */
    class BaseSynchronizationThread: public castor::server::IThread {
      
    public:
      
      /**
       * Default constructor
       */
      BaseSynchronizationThread() throw();

      /**
       * Default destructor
       */
      virtual ~BaseSynchronizationThread() throw() {};

      /// Not implemented
      virtual void init() {};

      /// Method called periodically to check whether files need to be deleted.
      // to be implemented on the NameServerSynchronizationThread 
      // and on the StagerSynchronizationThread
      virtual void run(void *param) = 0;

      /// Not implemented
      virtual void stop() {};

    protected:

      /* type of the SynchronizationThread:*/
      /* NamServerSynchronizationThread or StagerSynchronizationThread */
      unsigned int type;

      /* synchronization interval */      
      std::string syncIntervalConf; /* name of the parameter on the configurationFile */
      unsigned int syncInterval; /* to store the value */

      /* number of fileId to ask the NameServer/Stager for their existence in a round */
      std::string chunkSizeConf; /* name of the parameter on the configurationFile */
      unsigned int chunkSize; /* to store the value */
      

      /**
       * read config file values
       * @param syncInterval a pointer to the synchronization
       * interval values
       * @param chunkSize a pointer to the chunk size value
       * @param firstTime whether this is a first call. used
       * only for loggin purposes
       */
      virtual void readConfigFile(bool firstTime = false) throw();


      /**
       * synchronizes a list of files
       * @param nameServer the nameserver to use
       * @param fileIds a vector of fileIds
       * @param paths a map given the full file name for each fileid
       */
      virtual void synchronizeFiles(std::string nameServer,
                            const std::vector<u_signed64> &fileIds,
                            const std::map<u_signed64, std::string> &paths)
        throw() = 0;

    };

  } // End of namespace gc

} // End of namespace castor

#endif // GC_GCDAEMON_BASE_SYNCHRONIZATION_THREAD_HPP
