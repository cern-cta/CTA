/******************************************************************************
 *                      BaseObjectCInt.cpp
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
 * @(#)$RCSfile: BaseObjectCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/07/13 13:30:08 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/BaseObject.hpp"

extern "C" {

  //------------------------------------------------------------------------------
  // C_BaseObject_initLog
  //------------------------------------------------------------------------------
  int C_BaseObject_initLog(const char* name,
                           const unsigned long id) {
    try {
      castor::BaseObject::initLog(name, id);
    } catch (castor::exception::Exception e) {
      return -1;
    }
    return 0;
  }

} // End of extern "C"
