/******************************************************************************
 *                      stager_mapper.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: stager_mapper.h,v $ $Revision: 1.1 $ $Release$ $Date: 2005/05/25 14:29:44 $ $Author: bcouturi $
 *
 * 
 *
 * @author Ben Couturier
 *****************************************************************************/

/** @file $RCSfile: stager_mapper.h,v $
 * @version $Revision: 1.1 $
 * @date $Date: 2005/05/25 14:29:44 $
 */
/** @mainpage CASTOR Mapper
 * $RCSfile: stager_mapper.h,v $ $Revision: 1.1 $
 *
 * @section intro Introduction
 *
 * @section overview Overview
 *
 * 
 */

#ifndef stager_mapper_h
#define stager_mapper_h

#include <osdep.h>
#include <sys/types.h>


/** \addtogroup Functions */
/*\@{*/


////////////////////////////////////////////////////////////
//    Constants                                           //
////////////////////////////////////////////////////////////


#define USER_MAPPING_CATEGORY  "USTGMAP"
#define GROUP_MAPPING_CATEGORY "GSTGMAP"
#define STAGER_TYPE_CATEGORY   "STGTYPE"
#define STAGER_TYPE_V2         "V2"
#define STAGER_TYPE_V2_ALT     "v2"

////////////////////////////////////////////////////////////
//    stage_mapper_setenv                                 //
////////////////////////////////////////////////////////////

/**
 * stage_mapper_setenv
 * Sets the environment variable for a user acoording to its name and group
 * \ingroup Functions
 * @param username       The name of the user
 * @param groupname      The group to which the user belongs
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int DLL_DECL stager_mapper_setenv _PROTO((const char *username,
						   const char *groupname));

/*\@}*/

#endif /* stager_mapper_h */
