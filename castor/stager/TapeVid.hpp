/******************************************************************************
 *                      TapeVid.hpp
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
 * @(#)$RCSfile: TapeVid.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/04/05 11:51:33 $ $Author: sponcec3 $
 *
 * A very simple class representing a Tape VID
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_TAPEVID_HPP
#define STAGER_TAPEVID_HPP 1

namespace castor {

  namespace stager {

    /**
     * A very simple class representing a Tape VID
     */
    class TapeVid {

    public:

      /**
       * Constructor
       */
      TapeVid(char* vid) : m_vid(vid){};

      /**
       * getter
       */
      char* vid() { return m_vid; };

    private:

      /// The VID
      char* m_vid;

    };

  } // end of namespace stager

} // end of namespace castor

#endif // STAGER_TAPEVID_HPP
