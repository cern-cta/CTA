/******************************************************************************
 *                      BaseObject.h
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
 * @(#)$RCSfile: BaseObject.h,v $ $Revision: 1.1 $ $Release$ $Date: 2004/07/13 13:30:08 $ $Author: sponcec3 $
 *
 * C interface for BaseObject. It actually only contains
 * the initializer for the logging facilities
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_BASEOBJECT_H 
#define CASTOR_BASEOBJECT_H 1

/**
 * Defines which logging service should be used in
 * the future by giving a name and a service type.
 * Note that this method should only be called once.
 * In case of other calls, they will be ignored and
 * a warning will be issued in the already configured
 * log
 * @return -1 in case of error, 0 if successful
 */
int C_BaseObject_initLog(const char* name,
                         const unsigned long id);

#endif // CASTOR_BASEOBJECT_H
