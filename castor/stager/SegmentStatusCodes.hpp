/******************************************************************************
 *                      castor/stager/SegmentStatusCodes.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_SEGMENTSTATUSCODES_HPP
#define CASTOR_STAGER_SEGMENTSTATUSCODES_HPP

#ifdef __cplusplus
namespace castor {

  namespace stager {

#endif
    /**
     * enum SegmentStatusCodes
     * Possible status codes for a Segment
     */
    enum SegmentStatusCodes {
      SEGMENT_UNPROCESSED = 0,
      SEGMENT_FILECOPIED = 5,
      SEGMENT_FAILED = 6,
      SEGMENT_SELECTED = 7,
      SEGMENT_INVALID = 8
    }; // end of enum SegmentStatusCodes

    /**
     * Names of the differents representations, used to display
     * correctly the SegmentStatusCodes enum
     */
    extern const char* SegmentStatusCodesStrings[9];

#ifdef __cplusplus
  }; // end of namespace stager

}; // end of namespace castor

#endif
#endif // CASTOR_STAGER_SEGMENTSTATUSCODES_HPP
