
/******************************************************************************
 *                      MigHunterThread.hpp
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
 * @(#)$RCSfile: MigHunterThread.hpp,v $ $Author: waldron $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef MIGHUNTER_THREAD_HPP
#define MIGHUNTER_THREAD_HPP 1



#include "castor/infoPolicy/MigrationPySvc.hpp"
#include "castor/infoPolicy/CnsInfoMigrationPolicy.hpp"
#include "castor/infoPolicy/IPolicySvc.hpp"
#include "castor/infoPolicy/StreamPySvc.hpp"
#include "castor/server/BaseDbThread.hpp"

namespace castor {

  namespace rtcopy{
    namespace mighunter{

    /**
     * MigHunter  tread.
     */
    
      class MigHunterThread :public castor::server::BaseDbThread {
	castor::infoPolicy::IPolicySvc* m_policySvc;
	u_signed64 m_byteVolume;
	std::vector<std::string> m_listSvcClass;
        bool m_doClone;
	castor::infoPolicy::MigrationPySvc* m_migrSvc;
	castor::infoPolicy::StreamPySvc* m_strSvc;
        
      public:

      /**
       * constructor
       * @param maximum numbers of hours that an archived  request can stay in the datebase before being deleted.
       */
      MigHunterThread(castor::infoPolicy::IPolicySvc* svc, std::vector<std::string> svcClassArray, u_signed64 minByte, bool doClone,	castor::infoPolicy::MigrationPySvc* migrPy, castor::infoPolicy::StreamPySvc* StrPy);
      
      castor::infoPolicy::CnsInfoMigrationPolicy* getInfoFromNs(std::string nsHost,u_signed64 fileId);
      /**
       * destructor
       */
      virtual ~MigHunterThread() throw() {};

      /*For thread management*/

      virtual void run(void*);

    };

    } // end of mighunter
  } // end of namespace rtcopy

} // end of namespace castor

#endif // MIGHUNTER_THREAD_HPP
