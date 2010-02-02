/******************************************************************************
 *                      castor/tape/mighunter/MigrationPolicyElement.hpp
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
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef CASTOR_TAPE_MIGHUNTER_MIGRATIONPOLICYELEMENT_HPP
#define CASTOR_TAPE_MIGHUNTER_MIGRATIONPOLICYELEMENT_HPP


namespace castor {
namespace tape {
  namespace mighunter {

    struct MigrationPolicyElement {

      std::string policyName;

      std::string svcClassName;

      u_signed64 tapeCopyId;

      u_signed64 copyNb;

      std::string castorFileName;

      u_signed64 fileId;

      std::string tapePoolName;

      std::string nsHost;

      u_signed64 tapePoolId;

      int fileMode;

      int nlink;

      u_signed64 uid;

      u_signed64 gid;

      u_signed64 fileSize;

      u_signed64 aTime;

      u_signed64 mTime;

      u_signed64 cTime;

      int fileClass;

      unsigned char status;

    }; 
  }  /* end of namespace mighunter */
} /* end of namespace tape */
} /* end of namespace castor */

#endif // CASTOR_TAPE_MIGHUNTER_MIGRATIONPOLICYELEMENT_HPP
