/******************************************************************************
 *                      TimeStamp.hpp
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
 * @(#)$RCSfile: TimeStamp.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/04/05 13:36:36 $ $Author: sponcec3 $
 *
 * A simple object around a time stamp
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef DLF_TIMESTAMP_HPP
#define DLF_TIMESTAMP_HPP 1

// Include Files
#include <time.h>

namespace castor {

  namespace dlf {

    /**
     * A simple object around a time stamp
     */
    class TimeStamp {

    public:

      /**
       * Constructor
       */
      TimeStamp(time_t time) : m_time(time) {};

      /**
       * Accessor
       */
      int time() { return m_time; }

    private:

      /// the IP address, as an int
      int m_time;

    };

  } // end of namespace dlf

} // end of namespace castor

#endif // DLF_TIMESTAMP_HPP
