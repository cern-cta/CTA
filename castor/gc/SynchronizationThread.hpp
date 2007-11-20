/******************************************************************************
 *                      SynchronizationThread.hpp
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
 * @(#)$RCSfile: SynchronizationThread.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/20 17:19:37 $ $Author: sponcec3 $
 *
 * Thread going through the files stored on the CASTOR related
 * filesystem and checking their existence in the nameserver
 * In case they were dropped, the stager is informed and drops the file
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef GC_GCDAEMON_SYNCHRONIZATION_THREAD_HPP
#define GC_GCDAEMON_SYNCHRONIZATION_THREAD_HPP 1

// Include files
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"
#include <string>
#include <vector>

namespace castor {

  namespace gc {

    /**
     * Synchronization Thread
     */
    class SynchronizationThread: public castor::server::IThread {
      
    public:
      
      /**
       * Default constructor
       */
      SynchronizationThread();

      /**
       * Default destructor
       */
      virtual ~SynchronizationThread() throw() {};

      /// Not implemented
      virtual void init() {};

      /// Method called periodically to check whether files need to be deleted.
      virtual void run(void *param);

      /// Not implemented
      virtual void stop() {};

    private:

      /**
       * read config file values
       * @param syncInterval a pointer to the synchronization
       * interval values
       * @param chunkSize a pointer to the chunk size value
       * @param firstTime whether this is a first call. used
       * only for loggin purposes
       */
      void readConfigFile(unsigned int *syncInterval,
                          unsigned int * chunkSize,
                          bool firstTime = false) throw();

      /**
       * parse a fileName and extract the fileId
       * @param fileName the file name
       * @return a pair containing the nsHost and fileId
       * @throw exception in case the file name is not
       * matching the expected syntax
       */
      std::pair<std::string, u_signed64>
      fileIdFromFileName(char *fileName)
        throw (castor::exception::Exception);

      /**
       * synchronizes a list of files
       * @param nameServer the nameserver to use
       * @param fileIds a vector of fileIds
       * @param paths a map given the full file name for each fileid
       */
      void synchronizeFiles(std::string nameServer,
                            const std::vector<u_signed64> &fileIds,
                            const std::map<u_signed64, std::string> &paths)
        throw();

    };

  } // End of namespace gc

} // End of namespace castor

#endif // GC_GCDAEMON_SYNCHRONIZATION_THREAD_HPP
