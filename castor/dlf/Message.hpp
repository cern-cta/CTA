/******************************************************************************
 *                      Message.hpp
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
 * @(#)$RCSfile: Message.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/04/05 11:51:33 $ $Author: sponcec3 $
 *
 * Container for a DLF message
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef DLF_MESSAGE_HPP 
#define DLF_MESSAGE_HPP 1

// Include Files
#include <string>

namespace castor {

  namespace dlf {

    /**
     * Container for a DLF message
     */
    struct Message {
      /// Message number
      int number;
      /// Message text
      std::string text;
    }; 
    
  } // end of namespace dlf

} // end of namespace castor

#endif // DLF_MESSAGE_HPP
