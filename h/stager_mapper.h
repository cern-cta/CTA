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
 * @(#)$RCSfile: stager_mapper.h,v $ $Revision: 1.2 $ $Release$ $Date: 2005/05/25 16:09:22 $ $Author: bcouturi $
 *
 * 
 *
 * @author Ben Couturier
 *****************************************************************************/

/** @file $RCSfile: stager_mapper.h,v $
 * @version $Revision: 1.2 $
 * @date $Date: 2005/05/25 16:09:22 $
 */
/** @mainpage CASTOR Mapper
 * $RCSfile: stager_mapper.h,v $ $Revision: 1.2 $
 *
 * @section intro Introduction
 * The stage mapper consists of utility functions to help applications
 * switching between the stager v1 and stager v2 of CASTOR.
 * Its main usage is for the CASTOR SRM and GridFTP servers.
 *
 * @section overview Overview
 * Stage_mapper_setenv checks whether the user is mapped to a specific
 * stager/serviceclass in /etc/castor/stgmap.conf by the line
 * USTGMAP <username> <stager> [<pool>]
 * If no mapping is found, the methood will look for a group mapping
 * in the same file with the line:
 * USTGMAP <username> <stager> [<pool>] 
 * The environment variables STAGE_HOST, STAGE_POOL and STAGE_SVCCLASS
 * are set accordingly.
 *
 * If the user is mapped to a stager, then the method looks for the stager
 * in the list of V2 castor stagers in /etc/castor/stgtype.conf
 * If the is and entry:
 * STGTYPE <hostname> v2
 * or
 * STGTYPE <hostname> V2
 * then the method will define the RFIO_USE_CASTOR_V2 environment
 * variable.
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
EXTERN_C int DLL_DECL stage_mapper_setenv _PROTO((const char *username,
						   const char *groupname,
						   char **mstager,
						   char **msvcclass,
				                   int* isV2));

/*\@}*/

#endif /* stager_mapper_h */
