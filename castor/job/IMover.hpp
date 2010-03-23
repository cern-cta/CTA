/******************************************************************************
 *                      IMover.hpp
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
 * @(#)$RCSfile: IMover.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/26 14:54:54 $ $Author: waldron $
 *
 * Abstract interface for the Mover framework
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef DISKCOPY_IMOVER_HPP
#define DISKCOPY_IMOVER_HPP 1

// Include files
#include "castor/stager/DiskCopyInfo.hpp"
#include "castor/exception/Exception.hpp"


namespace castor {

  namespace job {

    /**
     * Abstract mover interface
     */
    class IMover {
      
    public:
      
      /**
       * Default destructor
       */
      virtual ~IMover() throw() {};

      /**
       * This method of the mover interface is invoked on the source end of a 
       * disk2disk copy transfer (the serving end).
       * @exception Exception in case of error
       */
      virtual void source() 
	throw(castor::exception::Exception) = 0;

      /**
       * This method of the mover interface is invoked on the destination end
       * of a disk2disk copy transfer. Unlike the source both the information
       * for the source and destination disk copies is proved as the mover
       * may need to know in advance where the source is in order to connect
       * to it.
       * @param diskCopy information about the destination disk copy
       * @param sourceDiskCopy information about the source disk copy
       * @exception Exception in case of error
       */
      virtual void destination
      (castor::stager::DiskCopyInfo *diskCopy,
       castor::stager::DiskCopyInfo *sourceDiskCopy) 
	throw(castor::exception::Exception) = 0;

      /**
       * Convenience method to stop the mover.
       * The default behaviour here is for the BaseMover to set the m_shutdown
       * attribute to true. This behaviour can be override to provide mover
       * specific functionality for process/transfer management.
       * @param immediate flag to indicate if stopping the mover should be done
       * immediate (i.e. non gracefully)
       * @exception Exception in case of error
       */
      virtual void stop
      (bool immediate)
	throw(castor::exception::Exception) = 0;

      /**
       * Returns the size of the source file represented in bytes. This is size
       * of the file on disk as given by a stat() call which may or may not be
       * difference to the size reported for the castorfile in the stager. 
       * Note: it is not mandatory for the mover to provide this information.
       */      
      virtual u_signed64 fileSize() = 0;

      /**
       * Returns the number of bytes transferred from the client to the source/
       * destination of a disk2disk copy. Note: it is not mandatory for the
       * mover to provide this information.
       */
      virtual u_signed64 bytesTransferred() = 0;

    };
    
  } // End of namespace job

} // End of namespace castor

#endif // DISKCOPY_IMOVER_HPP
