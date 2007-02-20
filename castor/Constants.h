/******************************************************************************
 *                      Constants.h
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
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_CONSTANTS_H 
#define CASTOR_CONSTANTS_H 1

#define C_ObjectsIds ObjectsIds
#define C_ServicesIds ServicesIds
#define C_RepresentationsIds RepresentationsIds

/**
 * Returns the name of a given type
 * @param type the type, as an id
 * @return the name or "UNKNOWN" if type is not in
 * the right range
 */
const char* C_ObjectsIdsStrings(unsigned int type);

/**
 * Returns the name of a given type
 * @param type the type, as an id
 * @return the name or "UNKNOWN" if type is not in
 * the right range
 */
const char* C_ServicesIdsStrings(unsigned int type);

/**
 * Returns the name of a given type
 * @param type the type, as an id
 * @return the name or "UNKNOWN" if type is not in
 * the right range
 */
const char* C_RepresentationsIdsStrings(unsigned int type);

#include "Constants.hpp"

#endif /* CASTOR_CONSTANTS_H */
