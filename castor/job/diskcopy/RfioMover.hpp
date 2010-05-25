/******************************************************************************
 *                      RfioMover.hpp
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
 * @(#)$RCSfile: RfioMover.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/08/22 13:04:37 $ $Author: kotlyar $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef RFIOMOVER_HPP
#define RFIOMOVER_HPP 1

// Include files
#include "castor/job/diskcopy/BaseMover.hpp"
#include "castor/job/diskcopy/IMover.hpp"


namespace castor {

  namespace job {

    namespace diskcopy {
    
      /**
       * Rfio Mover
       */
      class RfioMover : public castor::job::diskcopy::BaseMover {
      
      public:
      
        /**
         * Default constructor
         */
        RfioMover();
      
        /**
         * Destructor. Here the output and input file descriptors will be closed
         * if they are still open
         * @exception Exception in case of error
         */
        virtual ~RfioMover() throw();

        /**
         * This method of the mover interface is invoked on the source end of a 
         * disk2disk copy transfer (the serving end). For the RFIO mover we
         * simply exit. The rfiod process on the machine will listen to the
         * incoming connection from the destination end and serve the file 
         * @param diskCopy information about the destination disk copy
         * @exception Exception in case of error
         */
        virtual void source()
          throw(castor::exception::Exception);

        /**
         * This method of the mover interface is invoked on the destination end
         * of a disk2disk copy transfer. Unlike the source both the information
         * for the source and destination disk copies is provided as the mover
         * may need to know in advance where the source is in order to connect
         * to it. For RFIO this method is responsible for opening the source
         * and destination file descriptors and dealing with error conditions
         * thrown by the copyFile method
         * @param diskCopy information about the destination disk copy
         * @param sourceDiskCopy information about the source disk copy
         * @exception Exception in case of error
         */
        virtual void destination
        (castor::stager::DiskCopyInfo *diskCopy,
         castor::stager::DiskCopyInfo *sourceDiskCopy) 
          throw(castor::exception::Exception);

        /**
         * Convenience method to stop the mover.
         * @param immediate flag to indicate if stopping the mover should be
         * done immediate (i.e. non gracefully)
         * @exception Exception in case of error
         */
        virtual void stop(bool) throw(castor::exception::Exception) {};
      
        /**
         * Method responsible for reading data from the source/destination and
         * writing the data locally to disk.
         * @exception Exception in case of error
         */
        virtual void copyFile()
          throw(castor::exception::Exception);

        /**
         * The cleanup method is used to close all file descriptors associated
         * with a transfer and to gracefully handle error conditions. For
         * example, failure to close the destination/output file descriptor
         * will result in the removal/unlinking of the file.
         * @param silent indicates whether or not exceptions should be thrown
         * @param unlink indicates whether the file should be removed/unlinked
         * @exception Exception in case of error (only if silent is false)
         */
        virtual void cleanupFile(bool silent, bool unlink)
          throw(castor::exception::Exception);

      private:

        /// The RFIO output file
        std::string m_outputFile;

        /// The RFIO input file. This also contains the name of the remote
        /// diskserver.
        std::string m_inputFile;

        /// The output file descriptor pointing to the local file
        int m_outputFD;

        /// The input file descriptor. I.e. the descriptor of the source
        int m_inputFD;

        /// The total number of bytes to be transferred. I.e the size of the
        /// source/destination file
        u_signed64 m_totalBytes;
      
        /// The checksum type of the input file
        std::string m_csumType;
      
        /// The checksum value of the input file
        std::string m_csumValue;

      };

    } // End of namespace diskcopy

  } // End of namespace job

} // End of namespace castor

#endif // RFIOMOVER_HPP
