/******************************************************************************
 *                      castor/stager/TapeStatusCodes.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_TAPESTATUSCODES_HPP
#define CASTOR_STAGER_TAPESTATUSCODES_HPP

#ifdef __cplusplus
namespace castor {

  namespace stager {

#endif
    /**
     * enum TapeStatusCodes
     * Possible status codes for a Tape
     */
    enum TapeStatusCodes {
      TAPE_UNUSED = 0, //When a tape is not used by any stager
      TAPE_PENDING = 1, //TpInfo_getVIDsToDo() has not yet been called
      TAPE_WAITVDQM = 2, //VID has been submitted to VDQM
      TAPE_WAITMOUNT = 3, //RTCOPY request has started and TpFileInfo_anyReqsForVID() has returned >0
      TAPE_MOUNTED = 4, //Tape is mounted on drive
      TAPE_FINISHED = 5, //Processing of this tape is over
      TAPE_FAILED = 6, //Request failed
      TAPE_UNKNOWN = -1 //Unknown status code
    }; // end of enum TapeStatusCodes

#ifdef __cplusplus
  }; // end of namespace stager

}; // end of namespace castor

#endif
#endif // CASTOR_STAGER_TAPESTATUSCODES_HPP
