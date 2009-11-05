
/******************************************************************************
 *                      MigHunterDaemon.hpp
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
 * @(#)$RCSfile: MigHunterDaemon.hpp,v $ $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#ifndef MIGHUNTER_DAEMON_HPP
#define MIGHUNTER_DAEMON_HPP 1

// Include Files

#include "castor/server/BaseDaemon.hpp"
#include <list>

namespace castor {

  namespace rtcopy{
    namespace mighunter{

    /**
     * MigHunter daemon.
     */
    class MigHunterDaemon : public castor::server::BaseDaemon{
      u_signed64 m_timeSleep;
      u_signed64 m_byteVolume;
      std::list<std::string> m_listSvcClass;
      bool m_doClone;
      
    public:

      /**
       * constructor
       */
      MigHunterDaemon();
      inline u_signed64 timeSleep(){ return m_timeSleep;}
      inline u_signed64 byteVolume(){ return m_byteVolume;}
      inline const std::list<std::string>& listSvcClass(){return m_listSvcClass;}
      inline bool doClone(){return m_doClone;}

      inline void setTimeSleep(u_signed64 time){m_timeSleep=time;}
      inline void setByteVolume(u_signed64 bv){ m_byteVolume=bv;}
      inline void setListSvcClass(const std::list<std::string>& lsvc){m_listSvcClass=lsvc;}
      inline void setDoClone(bool dc){m_doClone=dc;}

      /**
       * destructor
       */
      virtual ~MigHunterDaemon() throw() {};
      void usage();
      void parseCommandLine(int argc, char* argv[]);

    };

    } //end of namespace mighunter
  } // end of namespace cleaning

} // end of namespace castor

#endif // MIGHUNTER_DAEMON_HPP
