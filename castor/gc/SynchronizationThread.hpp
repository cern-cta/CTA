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
 *
 * Thread going through the files stored on the CASTOR related filesystem and
 * checking their existence in the nameserver and in the stager catalog. In
 * case the files were dropped, the stager is informed and drops the file
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
#include <set>
#include <map>

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
       * Constructor
       * @param startDelay
       */
      SynchronizationThread(int startDelay);

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
       * Read config file values
       * @param syncInterval a pointer to the synchronization interval value
       * @param chunkInterval a pointer to the chunk interval value
       * @param chunkSize a pointer to the chunk size value
       * @param disableStagerSync a pointer to the boolean commanding disabling
       * of the synchronization with the stager
       * @param firstTime whether this is a first call. used only for logging
       * purposes
       */
      void readConfigFile(unsigned int *syncInterval,
			  unsigned int *chunkInterval,
                          unsigned int *chunkSize,
                          bool *disableStagerSync,
                          bool firstTime = false)
	throw(castor::exception::Exception);

      /**
       * Parse a fileName and extract the diskCopyId
       * @param fileName the file name
       * @return a pair containing the nsHost and diskCopyId
       * @throw exception in case the file name is not matching the expected
       * syntax
       */
      std::pair<std::string, u_signed64>
      diskCopyIdFromFileName(std::string fileName)
        throw (castor::exception::Exception);

      /**
       * Parse a filePath and extract the fileId
       * @param filePath the file path
       * @return the fileId of the file
       * @throw exception in case the file name is not matching the expected
       * syntax
       */
      u_signed64 fileIdFromFilePath(std::string filePath)
        throw (castor::exception::Exception);

      /**
       * gets a list of files open for write in the given mountPoint
       * @param mountPoint the mountPoint to be considered
       * @throw exception in case of error
       */
      std::set<std::string> getFilesBeingWrittenTo(char* mountPoint)
        throw(castor::exception::Exception);

      /**
       * Synchronizes a list of files from a given filesystem with the nameserver and stager catalog
       * @param nameServer the nameserver to use
       * @param paths a map giving the full file name for each diskCopyId to be checked
       * @param disableStagerSync whether to disable the stager synchronization
       * @param mountPoint the mountPoint of the filesystem on which the files reside
       */
      void synchronizeFiles(std::string nameServer,
                            const std::map<u_signed64, std::string> &paths,
                            bool disableStagerSync,
                            char* mountPoint)
        throw();

    private:

      /// The number of seconds to delay the first invocation of the run method
      int m_startDelay;

    };

  } // End of namespace gc

} // End of namespace castor

#endif // GC_GCDAEMON_SYNCHRONIZATION_THREAD_HPP
