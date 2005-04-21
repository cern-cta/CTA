/******************************************************************************
 *                      Constants.c
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
 * @(#)$RCSfile: ConstantsCInt.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2005/04/21 10:20:40 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"

extern "C" {
  
  // -----------------------------------------------------------------------
  // C_ObjectsIdsStrings
  // -----------------------------------------------------------------------
  const char* C_ObjectsIdsStrings(unsigned int type) {
    if (type < castor::ObjectsIdsNb) {
      return castor::ObjectsIdStrings[type];
    } else {
      return "UNKNOWN";
    }
  }
  
  // -----------------------------------------------------------------------
  // C_ServicesIdStrings
  // -----------------------------------------------------------------------
  const char* C_ServicesIdStrings(unsigned int type) {
    if (type < castor::ServicesIdsNb) {
      return castor::ServicesIdStrings[type];
    } else {
      return "UNKNOWN";
    }
  }

  // -----------------------------------------------------------------------
  // C_RepresentationsIdStrings
  // -----------------------------------------------------------------------
  const char* C_RepresentationsIdStrings(unsigned int type) {
    if (type < castor::RepresentationsIdsNb) {
      return castor::RepresentationsIdStrings[type];
    } else {
      return "UNKNOWN";
    }
  }
  
}
