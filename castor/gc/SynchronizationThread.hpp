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

#pragma once

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
       * @param firstTime whether this is a first call. used only for logging
       * purposes
       */
      void readConfigFile(bool firstTime = false);

      /**
       * Parse a fileName and extract the diskCopyId
       * @param fileName the file name
       * @return a pair containing the nsHost and diskCopyId
       * @throw exception in case the file name is not matching the expected
       * syntax
       */
      std::pair<std::string, u_signed64>
      diskCopyIdFromFileName(std::string fileName);

      /**
       * Parse a filePath and extract the fileId
       * @param filePath the file path
       * @return the fileId of the file
       * @throw exception in case the file name is not matching the expected
       * syntax
       */
      u_signed64 fileIdFromFilePath(std::string filePath);

      /**
       * synchronizes all fileSystems
       */
      void syncFileSystems();

      /**
       * synchronizes all dataPools
       */
      void syncDataPools();

      /**
       * Synchronizes a given local file
       * @param path the path where the file lies inside the mountPoint,
       *             or empty string for DataPools
       * @param fileName the file name
       * @param paths the differents "chunks", that is list of files, sorted by namespace
       * @return whether synchronization took place
       */
      bool syncLocalFile(const std::string &path,
                         const char* fileName,
                         std::map<std::string, std::map<u_signed64, std::string> > &paths);

      /**
       * Synchronizes a given ceph file
       * @param fileName the name of the file
       * @param paths the differents "chunks", that is list of files, sorted by namespace
       * @return whether synchronization took place
       */
      bool syncCephFile(const std::string fileName,
                        std::map<std::string, std::map<u_signed64, std::string> > &paths);

      /**
       * Synchronizes all chunks present in paths
       * @param paths the differents "chunks", that is list of files, sorted by namespace
       */
      void syncAllChunks(std::map<std::string, std::map<u_signed64, std::string> > &paths);

      /**
       * Checks whether to synchronize a chunk for the given nameserver
       * and does it if needed
       * @param nameServer the nameServer concerned
       * @param paths the differents "chunks", that is list of files, sorted by namespace
       * @param minimumNbFiles the minimumNbFiles we should have in the chunk to
       *        got for synchronization
       * @return whether synchronization took place
       */
      bool checkAndSyncChunk(const std::string &nameServer,
                             std::map<std::string, std::map<u_signed64, std::string> > &paths,
                             u_signed64 minimumNbFiles);

      /**
       * Synchronizes a list of files from a given filesystem with the nameserver and stager catalog
       * @param nameServer the nameserver to use
       * @param paths a map giving the full file name for each diskCopyId to be checked
       */
      void synchronizeFiles(const std::string &nameServer,
                            const std::map<u_signed64, std::string> &paths)
        throw();

    private:

      /// The number of seconds to delay the first invocation of the run method
      int m_startDelay;
      /// The number of seconds to wait between two chunks' synchronization
      unsigned int m_chunkInterval;
      /// the size of a chunk, aka the maximum number of files to synchronize in one go
      unsigned int m_chunkSize;
      /// the grace period for new files. That is the period during which they are not
      /// considered for synchronization
      unsigned int m_gracePeriod;
      /// Whether stager synchronization should be disabled;
      bool m_disableStagerSync;

    };

  } // End of namespace gc

} // End of namespace castor

