/******************************************************************************
 *                      IRequestHandler.h
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
 * @(#)$RCSfile: IRequestHandler.h,v $ $Revision: 1.1 $ $Release$ $Date: 2004/06/16 14:53:27 $ $Author: sponcec3 $
 *
 * C Interface for the IRequestHandler object
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_IREQUESTHANDLER_H 
#define CASTOR_IREQUESTHANDLER_H 1

/// Forward declarations for the C world
struct C_IService_t;
struct C_IAddress_t;
struct C_IRequestHandler_t;

/**
 * Dynamic cast from IService
 */
struct C_IRequestHandler_t*
C_IRequestHandler_fromIService(struct C_IService_t* obj);

/**
 * Destructor
 */
int C_IRequestHandler_delete(struct C_IRequestHandler_t* rhsvc);

/**
 * returns an address to the next request to handle in
 * the database.
 * Note that the caller is responsible for the deallocation
 * of the address. Note also that the database transaction
 * is not commited after this call. The caller is responsible
 * for the commit once it made sure that no request can be
 * forgotten whatever happens.
 * @param rhSvc the IRequestHandler used
 * @param address the address of the next request
 * @return 0 upon success, -1 in case of error.
 * In such case, serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * C_IRequestHandler_errorMsg
 */
int C_IRequestHandler_nextRequestAddress(struct C_IRequestHandler_t* rhsvc,
                                         struct C_IAddress_t** address);

/**
 * Gives the number of requests handle in the database.
 * @param nbReq the number pf requests
 * @param rhSvc the IRequestHandler used
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * C_IRequestHandler_errorMsg
 */
int C_IRequestHandler_nbRequestsToProcess(struct C_IRequestHandler_t* rhsvc,
                                          unsigned int *nbReq);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @param rhSvc the IRequestHandler used
 * @return the error message
 */
const char* C_IRequestHandler_errorMsg(struct C_IRequestHandler_t* rhSvc);


#endif // CASTOR_IREQUESTHANDLER_H
