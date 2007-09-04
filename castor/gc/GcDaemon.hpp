/******************************************************************************
 *                      GcDaemon.hpp
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
 * @(#)GcDaemon.hpp,v 1.2 $Release$ 2005/03/16 10:37:01 jiltsov
 *
 * Garbage collector daemon handling the deletion of local
 * files on a filesystem. Makes remote calls to the stager's
 * GC service to know what to delete and to update the catalog
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef GC_GCDAEMON_HPP
#define GC_GCDAEMON_HPP 1

// Include files
#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace gc {

    /**
     * Garbage Collector daemon.
     */
    class GcDaemon: public castor::server::BaseDaemon {

    public:

      /**
       * Default constructor
       */
      GcDaemon();

      /**
       * Default destructor
       */
      virtual ~GcDaemon() throw() {};

    };

  } // End of namespace gc

} // End of namespace castor

#endif // GC_GCDAEMON_HPP
