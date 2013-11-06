/******************************************************************************
 *                      DeletionThread.hpp
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
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef GC_GCDAEMON_DELETION_THREAD_HPP
#define GC_GCDAEMON_DELETION_THREAD_HPP 1

// Include files
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"


namespace castor {

  namespace gc {

    /**
     * Deletion Thread
     */
    class DeletionThread: public castor::server::IThread {

    public:

      /**
       * Default constructor
       */
      DeletionThread();

      /**
       * Constructor 
       * @param startDelay
       */
      DeletionThread(int startDelay);

      /**
       * Default destructor
       */
      virtual ~DeletionThread() throw() {};

      /// Not implemented
      virtual void init() {};

      /// Method called periodically to check whether files need to be deleted.
      virtual void run(void *param);

      /// Not implemented
      virtual void stop() {};

    private:

      /**
       * Actually remove the file from local filesystem.
       * @param filepath the path of the file to remove
       * @param filesize the size of the file
       * @param fileage the age of the file
       * @return the size (in bytes) of the removed file
       * @exception when the removing failed
       */
      void gcRemoveFilePath
      (std::string filepath, u_signed64 &filesize, u_signed64 &fileage)
	throw (castor::exception::Exception);

    private:

      /// The name of the diskserver on which the GC daemon is running
      std::string m_diskServerName;

      /// The interval at which the GC's main loop runs
      int m_interval;

      /// The number of seconds to delay the first invocation of the run method
      int m_startDelay;

    };

  } // End of namespace gc

} // End of namespace castor

#endif // GC_GCDAEMON_DELETION_THREAD_HPP
