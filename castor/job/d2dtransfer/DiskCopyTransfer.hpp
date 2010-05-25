/******************************************************************************
 *                      DiskCopyTransfer.hpp
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
 * @(#)$RCSfile: DiskCopyTransfer.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/26 14:54:55 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef DISKCOPY_TRANSFER_HPP
#define DISKCOPY_TRANSFER_HPP 1

// Include files
#include "castor/server/BaseDaemon.hpp"

namespace castor {

  namespace job {

    namespace d2dtransfer {

      /**
       * DiskCopyTransfer class
       */
      class DiskCopyTransfer: public castor::server::BaseDaemon {
	
      public:
	
	/**
	 * Default constructor
	 */
	DiskCopyTransfer();
	
	/**
       * Default destructor
       */
	virtual ~DiskCopyTransfer() throw() {};
	
      };

    } // End of namespace d2dtransfer

  } // End of namespace job

} // End of namespace castor

#endif // DISKCOPY_TRANSFER_HPP
