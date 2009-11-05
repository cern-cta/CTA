
/******************************************************************************
 *                      StreamThread.hpp
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
 * @(#)$RCSfile: StreamThread.hpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef STREAM_THREAD_HPP
#define STREAM_THREAD_HPP 1




#include "castor/infoPolicy/StreamPySvc.hpp"
#include "castor/server/BaseDbThread.hpp"
#include <list>
namespace castor {

  namespace rtcopy{
    namespace mighunter{

    /**
     * Stream  tread.
     */
    
      class StreamThread :public castor::server::BaseDbThread {
  
	castor::infoPolicy::StreamPySvc* m_strSvc;
	std::list<std::string> m_listSvcClass;
      public:

      /**
       * constructor
       * @param maximum numbers of hours that an archived  request can stay in the datebase before being deleted.
       */
      StreamThread(std::list<std::string> svcClassArray, castor::infoPolicy::StreamPySvc* StrPy);
     
      /**
       * destructor
       */
      virtual ~StreamThread() throw() {};

      /*For thread management*/

      virtual void run(void*);

    };

    } // end of mighunter
  } // end of namespace rtcopy

} // end of namespace castor

#endif // STREAM_THREAD_HPP
