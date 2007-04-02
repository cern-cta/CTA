/******************************************************************************
 *                castor/db/ora/BaseTapeSvc.hpp
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
 * @(#)$RCSfile: BaseTapeSvc.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/04/02 15:26:05 $ $Author: sponcec3 $
 *
 * Basic implementation of ITapeSvc.
 * This class only implements the functions that are not DB related.
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef STAGER_BASETAPESVC_HPP
#define STAGER_BASETAPESVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/stager/ITapeSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/monitoring/StreamDirection.hpp"
#include <string>

namespace castor {

  namespace stager {

    /**
     * Basic implementation of the ITapeSvc.
     * This class only implements the functions that are not DB related
     */
    class BaseTapeSvc : public virtual castor::stager::ITapeSvc {

    public:

      /**
       * default constructor
       */
      BaseTapeSvc() throw(castor::exception::Exception);

      /**
       * Sends a UDP message to the rmMasterDaemon to inform it
       * of the creation or deletion of a stream on a given
       * machine/filesystem. This method is always successful
       * although there is no guaranty that the UDP package is
       * sent and arrives.
       * @param diskserver the diskserver where the stream resides
       * @param filesystem the filesystem where the stream resides
       * @param direction the stream direction (read, write or readWrite)
       * @param created whether the stream was created or deleted
       */
      virtual void sendStreamReport(const std::string diskServer,
				    const std::string fileSystem,
				    const castor::monitoring::StreamDirection direction,
				    const bool created) throw();

    private:

      /// the rmMaster host
      std::string m_rmMasterHost;

      /// the rmMaster port
      int m_rmMasterPort;

    }; // end of class BaseTapeSvc

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_BASETAPESVC_HPP
