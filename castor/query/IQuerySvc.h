/******************************************************************************
 *                      IQuerySvc.h
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
 * @(#)$RCSfile: IQuerySvc.h,v $ $Revision: 1.3 $ $Release$ $Date: 2005/05/19 15:53:39 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef QUERY_IQUERYSVC_H 
#define QUERY_IQUERYSVC_H 1

/// Forward declarations for the C world
struct C_IService_t;
struct Cquery_IQuerySvc_t;
struct Cstager_DiskCopyForRecall_t;

/**
 * Dynamic cast from IService
 */
struct Cquery_IQuerySvc_t*
Cquery_IQuerySvc_fromIService(struct C_IService_t* obj);

/**
 * Destructor
 */
int Cquery_IQuerySvc_delete(struct Cquery_IQuerySvc_t* svcs);

/**
 * Gets all DiskCopies for a given file.
 * The caller is responsible for the deallocation of
 * the returned objects
 * @param qrySvc the IQuerySvc used
 * @param fileId the fileId identifying the file
 * @param nsHost the name server host for this file
 * @param svcClassId the Id of the service class we're using
 * @param diskCopies the list of DiskCopies available
 * @param diskCopiesNb the number of DiskCopies in the list
 * @return >0 : at least one tapeCopy found. 0 : no Tapecopy found
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cquery_IQuerySvc_errorMsg
 */
int Cquery_IQuerySvc_diskCopies4File
(struct Cquery_IQuerySvc_t* qrySvc,
 char* fileId, char* nsHost,
 u_signed64 svcClassId,
 struct Cstager_DiskCopyForRecall_t*** diskCopies,
 unsigned int* diskCopiesNb);

#endif // QUERY_IQUERYSVC_H
