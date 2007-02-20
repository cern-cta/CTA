/******************************************************************************
 *                      IFSSvc.h
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
 * @(#)$RCSfile: IFSSvc.h,v $ $Revision: 1.2 $ $Release$ $Date: 2007/02/20 17:55:56 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_IFSSVC_H
#define CASTOR_IFSSVC_H 1

#include "castor/Constants.h"
#include "castor/stager/DiskCopyStatusCodes.h"

/* Forward declarations for the C world */
struct C_IService_t;
struct C_IClient_t;
struct C_IObject_t;
struct C_IAddress_t;
struct Cstager_IFSSvc_t;
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
struct Cstager_IFSSvc_t*
Cstager_IFSSvc_fromIService(struct C_IService_t* obj);

/**
 * Destructor
 */
int Cstager_IFSSvc_delete(struct Cstager_IFSSvc_t* svcs);

/**
 * Selects the next request the stager should deal with.
 * Selects a Request in START status and move its status
 * PROCESSED to avoid double processing.
 * The selection is restricted to Request of a given set
 * of types.
 * @param fsSvc the IFSSvc used
 * @param types the list of accepted types for the request
 * @param nbTypes the number of types in the list
 * @param request the request to process
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_requestToDo
(struct Cstager_IFSSvc_t* fsSvc,
 enum C_ObjectsIds* types,
 unsigned int nbTypes,
 struct Cstager_Request_t** request);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @param fsSvc the IFSSvc used
 * @return the error message
 */
const char* Cstager_IFSSvc_errorMsg(struct Cstager_IFSSvc_t* fsSvc);

/**
 * Retrieves a SvcClass from the database based on its name.
 * Caller is in charge of the deletion of the allocated object
 * @param fsSvc the IFSSvc used
 * @param svcClass the SvcClass object returned, or 0 if none found
 * @param name the name of the SvcClass
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_selectSvcClass(struct Cstager_IFSSvc_t* fsSvc,
                                      struct Cstager_SvcClass_t** svcClass,
                                      const char* name);

/**
 * Retrieves a FileClass from the database based on its name.
 * Caller is in charge of the deletion of the allocated object
 * @param fsSvc the IFSSvc used
 * @param fileClass the FileClass object returned, or 0 if none found
 * @param name the name of the FileClass
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_selectFileClass(struct Cstager_IFSSvc_t* fsSvc,
                                   struct Cstager_FileClass_t** fileClass,
                                   const char* name);

/**
 * Retrieves a FileSystem from the database based on its
 * mount point and diskServer name.
 * Caller is in charge of the deletion of the allocated
 * objects, including the DiskServer Object
 * @param fsSvc the IFSSvc used
 * @param mountPoint the mountPoint of the FileSystem
 * @param diskServer the name of the disk server hosting this file system
 * @param fileSystem the FileSystem linked to its DiskServer, or 0 if none found
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_selectFileSystem(struct Cstager_IFSSvc_t* fsSvc,
                                    struct Cstager_FileSystem_t** fileSystem,
                                    const char* mountPoint,
                                    const char* diskServer);

/**
 * Retrieves a DiskPool from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param fsSvc the IFSSvc used
 * @param diskPool the DiskPool object returned, or 0 if none found
 * @param name the name of the DiskPool
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_selectDiskPool(struct Cstager_IFSSvc_t* fsSvc,
                                  struct Cstager_DiskPool_t** diskPool,
                                  const char* name);

/**
 * Retrieves a TapePool from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param fsSvc the IFSSvc used
 * @param tapePool the TapePool object returned, or 0 if none found
 * @param name the name of the TapePool
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_selectTapePool(struct Cstager_IFSSvc_t* fsSvc,
                                  struct Cstager_TapePool_t** tapePool,
                                  const char* name);

/**
 * Retrieves a DiskServer from the database based on name.
 * Caller is in charge of the deletion of the allocated
 * memory.
 * @param fsSvc the IFSSvc used
 * @param diskServer the DiskServer object returned, or 0 if none found
 * @param name the name of the DiskServer
 * @return 0 : OK.
 * -1 : an error occurred and serrno is set to the corresponding error code
 * A detailed error message can be retrieved by calling
 * Cstager_IFSSvc_errorMsg
 */
int Cstager_IFSSvc_selectDiskServer(struct Cstager_IFSSvc_t* fsSvc,
                                    struct Cstager_DiskServer_t** diskServer,
                                    const char* name);

#endif // CASTOR_IFSSVC_H
