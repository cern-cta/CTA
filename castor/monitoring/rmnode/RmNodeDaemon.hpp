/******************************************************************************
 *                      RmNodeDaemon.hpp
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
 * @(#)$RCSfile$ $ $Author $
 *
 * This daemons collects data concerning its node and sends them to
 * the monitoring master daemon
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMNODE_RMNODEDAEMON_HPP
#define RMNODE_RMNODEDAEMON_HPP 1

// Include Files

#include "castor/server/BaseDaemon.hpp"

namespace castor {

  namespace monitoring{

    namespace rmnode{

      /**
       * Castor RmNode daemon.
       */
      class RmNodeDaemon : public castor::server::BaseDaemon {

      public:

        /**
         * constructor
         */
        RmNodeDaemon();

        /**
         * destructor
         */
        virtual ~RmNodeDaemon() throw() {};

      };

    } // end of namespace rmnode

  } // end of namespace monitoring

} // end of namespace castor

#endif // RMNODE_RMNODEDAEMON_HPP
