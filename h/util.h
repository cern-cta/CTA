/******************************************************************************
 *                      util.h
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
 * @(#)$RCSfile: util.h,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:28 $ $Author: sponcec3 $
 *
 * Utilities for castor, including som parsing functions
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef H_UTIL_H 
#define H_UTIL_H 1

#include <osdep.h>

/**
 * Checks whether the given string is a size.
 * The accepted syntax is :
 *    [blanks]<digits>[b|k|M|G|T|P]
 * @return -1 if it is not a size,
 * 0 if it is a size with no unit,
 * 1 if it is a size with a unit
 */
EXTERN_C int  DLL_DECL check_for_strutou64 _PROTO((char*));

#endif // H_UTIL_H
