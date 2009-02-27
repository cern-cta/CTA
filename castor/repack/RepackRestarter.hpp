/******************************************************************************
 *                      RepackRestarter.hpp
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
 * @(#)$RCSfile: RepackRestarter.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2009/02/27 10:25:35 $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#ifndef _REPACKRESTARTER_HPP_
#define _REPACKRESTARTER_HPP_


#include "castor/server/IThread.hpp"
#include "RepackServer.hpp"

namespace castor {

  namespace repack {

    /** forward declaration */
    class RepackServer;


    /**
     * class RepackRestarter
     */
    class RepackRestarter : public castor::server::IThread
    {
    public :

      /**
       * Constructor.
       * @param srv The refernce to the constructing RepackServer Instance.
       */
      RepackRestarter(RepackServer* srv);

      /**
       * Destructor
       */
      ~RepackRestarter() throw();

      ///empty initialization
      virtual void init() {};

      /**
       * Inherited from IThread : not implemented
       */
      virtual void stop() throw();

      /**
       * Checks the DB for new RepackSubRequests in TOBESTAGED/RESTART and
       * performs the procedure.
       * (execute startRepack/restartRepack method, described below).
       */
      virtual void run(void* param) throw();


    private:

      /**
       * A pointer to the server instance, which keeps information
       * about Nameserver, stager etc.
       */
      RepackServer* ptr_server;
    };



  } //END NAMESPACE REPACK
} //END NAMESPACE CASTOR



#endif //_REPACKRESTARTER_HPP_
