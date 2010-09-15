/******************************************************************************
 *                      castor/tape/rechandler/RecallPolicyElement.hpp
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

#ifndef CASTOR_TAPE_RECHANDLER_RECALLPOLICYELEMENT_HPP
#define CASTOR_TAPE_RECHANDLER_RECALLPOLICYELEMENT_HPP



namespace castor     {
namespace tape       {
namespace rechandler {

  struct RecallPolicyElement {

      std::string policyName;

      std::string svcClassName;

      u_signed64 totalBytes;

      u_signed64 numSegments;

      u_signed64 ageOfOldestSegment;

      std::string vid;

      u_signed64 tapeId;

      u_signed64 priority;

      u_signed64 status;

    }; /* end of class RecallPolicyElement */

} /* end of namespace rechandler */
} /* end of namespace tape */
} /* end of namespace castor */

#endif // CASTOR_TAPE_RECHANDLER_RECALLPOLICYELEMENT_HPP
