/******************************************************************************
 *                      HandlerData.hpp
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
 * @(#)$RCSfile: HandlerData.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/02/22 08:57:52 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef SCHEDULER_HANDLER_DATA_HPP
#define SCHEDULER_HANDLER_DATA_HPP 1

// Include files
#include "castor/exception/Exception.hpp"
#include "Cuuid.h"
#include "Cns_api.h"
#include <string>
#include <vector>


namespace castor {

  namespace scheduler {

    /**
     * An object holding the hander data for a single job
     */
    class HandlerData {
      
    public:

      /**
       * Default constructor
       */
      HandlerData();

      /**
       * Simple constructor which supports the parsing of the jobs external
       * resource requirements
       * @param resreq pointer to the internal resource data structure of the
       * job
       * @exception Exception in case of error
       */
      HandlerData(void *resreq)
	throw(castor::exception::Exception);
      
      /**
       * Default destructor
       */
      virtual ~HandlerData() throw() {};

      /// The request uuid of the job
      Cuuid_t reqId;

      /// The sub request uuid of the job. Also known as the job name
      Cuuid_t subReqId;

      /// The nameserver invariant
      Cns_fileid fileId;

      /// The name of the job
      std::string jobName;

      /// The protocol that will be used to access the file
      std::string protocol;

      /// The expected size of the castor file
      u_signed64 xsize;

      /// The name of the service class
      std::string svcClass;

      /// The filesystems requested to fulfil the jobs resource requirements 
      /// in the scheduler
      std::string requestedFileSystems;

      /// A tokenized list of requested filesystems 
      std::vector<std::string> rfs;

      /// The direction of the transfer, e.g. read, write, read/write
      std::string openFlags;

      /// The type of the request
      int requestType;
      
      /// The name of the source diskserver to be used in a d2d transfer
      std::string sourceDiskServer;

      /// The name of the source filesystem to be used in a d2d transfer
      std::string sourceFileSystem;

      /// The service class name of the source diskcopy
      std::string sourceSvcClass;

      /// The location of the notification file
      std::string notifyFile;

      /// The diskserver selected to run the job as chosen by the match phase
      std::string selectedDiskServer;

      /// The filesystem selected to run the job as chosen by the match phase
      std::string selectedFileSystem;

      /// The LSF job id
      int jobId;

      /// The number of match phases performed by the job
      u_signed64 matches;

    };

  } // End of namespace scheduler

} // End of namespace castor

#endif // SCHEDULER_HANDLER_DATA_HPP
