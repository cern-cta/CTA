/******************************************************************************
 *                      Collector.hpp
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
 * @(#)$RCSfile: Collector.hpp,v $ $Author: sponcec3 $
 *
 * The Collector thread of the RmMaster daemon.
 * It collects the data from the different nodes and updates a shared
 * memory representation of it
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMMASTER_COLLECTOR_HPP
#define RMMASTER_COLLECTOR_HPP 1

#include "castor/server/IThread.hpp"

namespace castor {

  // Forward declarations
  namespace stager {
    class IStagerSvc;
  }

  namespace rmmaster {

    // Forward declarations
    class ClusterStatus;

    /**
     * Collector  tread.
     */
    class Collector : public castor::server::IThread {

    public:

      /**
       * constructor/*
       * @param maximum numbers of days that a request can stay in the datebase before being deleted.
       */

      Collector(castor::stager::IStagerSvc* svc,
                castor::rmmaster::ClusterStatus* clusterStatus);

      /**
       * destructor
       */
      virtual ~Collector() throw();

      /*For thread management*/
      void run(void*) throw();
      void stop() throw();

    private:
      
      // stager service to call the oracle procedures
      castor::stager::IStagerSvc* m_stgService;

      // Machine Status List
      castor::rmmaster::ClusterStatus* m_clusterStatus;

    };

  } // end of namespace cleaning

} // end of namespace castor

#endif // RMMASTER_COLLECTOR_HPP
