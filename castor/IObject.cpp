/******************************************************************************
 *                      IObject.cpp
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
 * @(#)$RCSfile: IObject.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/11/29 16:04:26 $ $Author: sponcec3 $
 *
 * Implementation of the streaming operator << for IObject
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"

//-----------------------------------------------------------------------------
// operator<<
//-----------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& s, const castor::IObject& obj) {
  castor::ObjectSet set;
  obj.print(s, "", set);
  return s;
}

