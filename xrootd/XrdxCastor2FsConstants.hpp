/*******************************************************************************
 *                      XrdxCastor2Stager.hh
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2012  CERN
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
 *
 * @author Elvin Sindrilaru & Andreas Peters - CERN
 * 
 ******************************************************************************/

#ifndef __XCASTOR_FSCONSTANTS_HH__
#define __XCASTOR_FSCONSTANTS_HH__

#define XCASTOR2FS_GRIDMAPCHECKINTERVAL     60 // in seconds
#define XCASTOR2FS_METADATAUPDATEINTERVAL    1 // in seconds
#define XCASTOR2FS_MAXFILESYSTEMS         8192
#define XCASTOR2FS_MAXDISTINCTUSERS       8192

//! Constants dealing with async requests for the stager 
#define XCASTOR2FS_CALLBACKPORT          30000 ///< async requests callback port
#define XCASTOR2FS_RESP_TIMEOUT            600 ///< timeout for async responses 
#define XCASTOR2FS_MAX_REQUESTS           2000 ///< max no. of requests on-the-fly

#endif // __XCASTOR_FSCONSTANTS_HH__
