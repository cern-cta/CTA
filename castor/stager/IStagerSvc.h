/******************************************************************************
 *                      IStagerSvc.h
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

#ifndef CASTOR_ISTAGERSVC_H
#define CASTOR_ISTAGERSVC_H 1

#include "castor/Constants.h"
#include "castor/stager/DiskCopyStatusCodes.h"

/* Forward declarations for the C world */
struct C_IService_t;
struct C_IClient_t;
struct C_IObject_t;
struct C_IAddress_t;
struct Cstager_IStagerSvc_t;
struct Cstager_Tape_t;
struct Cstager_Stream_t;
struct Cstager_Request_t;
struct Cstager_Segment_t;
struct Cstager_DiskCopy_t;
struct Cstager_DiskPool_t;
struct Cstager_SvcClass_t;
struct Cstager_TapePool_t;
struct Cstager_CastorFile_t;
struct Cstager_DiskServer_t;
struct Cstager_SubRequest_t;
struct Cstager_FileSystem_t;
struct Cstager_TapeCopyForMigration_t;
struct Cstager_DiskCopyForRecall_t;

/**
 * Dynamic cast from IService
 */
struct Cstager_IStagerSvc_t*
Cstager_IStagerSvc_fromIService(struct C_IService_t* obj);

/**
 * Destructor
 */
int Cstager_IStagerSvc_delete(struct Cstager_IStagerSvc_t* svcs);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @param stgSvc the IStagerSvc used
 * @return the error message
 */
const char* Cstager_IStagerSvc_errorMsg(struct Cstager_IStagerSvc_t* stgSvc);

/**
 * Retrieves a SvcClass from the database based on its name.
 * Caller is in charge of the deletion of the allocated object
 * @param stgSvc the IStagerSvc used
 * @param svcClass the SvcClass object returned, or 0 if none found
 * @param name the name of the SvcClass
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectSvcClass(struct Cstager_IStagerSvc_t* stgSvc,
                                      struct Cstager_SvcClass_t** svcClass,
                                      const char* name);

/**
 * Retrieves a FileClass from the database based on its name.
 * Caller is in charge of the deletion of the allocated object
 * @param stgSvc the IStagerSvc used
 * @param fileClass the FileClass object returned, or 0 if none found
 * @param name the name of the FileClass
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectFileClass(struct Cstager_IStagerSvc_t* stgSvc,
                                       struct Cstager_FileClass_t** fileClass,
                                       const char* name);

/**
 * Retrieves a DiskPool from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param stgSvc the IStagerSvc used
 * @param diskPool the DiskPool object returned, or 0 if none found
 * @param name the name of the DiskPool
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectDiskPool
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_DiskPool_t** diskPool,
 const char* name);

/**
 * Retrieves a TapePool from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param stgSvc the IStagerSvc used
 * @param tapePool the TapePool object returned, or 0 if none found
 * @param name the name of the TapePool
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IStagerSvc_errorMsg
 */
int Cstager_IStagerSvc_selectTapePool
(struct Cstager_IStagerSvc_t* stgSvc,
 struct Cstager_TapePool_t** tapePool,
 const char* name);

#endif /* CASTOR_ISTAGERSVC_H */
