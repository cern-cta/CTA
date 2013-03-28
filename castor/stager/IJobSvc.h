/******************************************************************************
 *                      IJobSvc.h
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
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_IJOBSVC_H
#define CASTOR_IJOBSVC_H 1

#include "castor/Constants.h"

/**
 * Prepares a file for migration, when needed.
 * This is called both when a stagePut is over and when a
 * putDone request is processed.
 * In the case of a stagePut that in part of a PrepareToPut,
 * it actually does not prepare the file for migration
 * but only updates its size in DB and name server.
 * Otherwise (stagePut with no prepare and putDone),
 * it also updates the filesystem free space and creates
 * the needed TapeCopies according to the FileClass of the
 * castorFile.
 * @param subReqId The id of the SubRequest
 * @param fileSize The actual size of the castor file
 * @param timeStamp To know if the fileSize is still valid
 * @param fileId the id of the castorFile
 * @param nsHost the name server hosting this castorFile
 * @param csumtype the checksum type of the castor file
 * @param csumvalue the checksum value of the castor file
 * @param errorCode error code in case of error
 * @param errorMsg error message in case of error. Note that
 * memory is allocated for the message and that the caller is
 * responsible for releasing it
 * @return 0 in case of success, -1 otherwise.
 * If -1, then errorCode and errorMsg are filled.
 */
int Cstager_IJobSvc_prepareForMigration
(u_signed64 subReqId,
 u_signed64 fileSize,
 u_signed64 timeStamp,
 u_signed64 fileId,
 const char* nsHost,
 const char* csumtype,
 const char* csumvalue,
 int* errorCode,
 char** errorMsg);

/**
 * Informs the stager the a Get or Update SubRequest
 * (without write) was finished successfully.
 * The SubRequest and potentially the corresponding
 * Request will thus be removed from the DataBase
 * @param subReqId the id of the finished SubRequest
 * @param fileId the id of the castorFile
 * @param nsHost the name server hosting this castorFile
 * @param errorCode error code in case of error
 * @param errorMsg error message in case of error. Note that
 * memory is allocated for the message and that the caller is
 * responsible for releasing it
 * @return 0 in case of success, -1 otherwise.
 * If -1, then errorCode and errorMsg are filled.
 */
int Cstager_IJobSvc_getUpdateDone
(u_signed64 subReqId,
 u_signed64 fileId,
 const char* nsHost,
 int* errorCode,
 char** errorMsg);

/**
 * Informs the stager the a Get or Update SubRequest
 * (without write) failed.
 * The SubRequest's status will thus be set to FAILED
 * @param subReqId the id of the failing SubRequest
 * @param fileId the id of the castorFile
 * @param nsHost the name server hosting this castorFile
 * @param errorCode error code in case of error
 * @param errorMsg error message in case of error. Note that
 * memory is allocated for the message and that the caller is
 * responsible for releasing it
 * @return 0 in case of success, -1 otherwise.
 * If -1, then errorCode and errorMsg are filled.
 */
int Cstager_IJobSvc_getUpdateFailed
(u_signed64 subReqId,
 u_signed64 fileId,
 const char* nsHost,
 int* errorCode,
 char** errorMsg);

/**
 * Informs the stager the a Put or PutDone SubRequest failed.
 * The SubRequest's status will thus be set to FAILED
 * @param subReqId the id of the failing SubRequest
 * @param fileId the id of the castorFile
 * @param nsHost the name server hosting this castorFile
 * @param errorCode error code in case of error
 * @param errorMsg error message in case of error. Note that
 * memory is allocated for the message and that the caller is
 * responsible for releasing it
 * @return 0 in case of success, -1 otherwise.
 * If -1, then errorCode and errorMsg are filled.
 */
int Cstager_IJobSvc_putFailed
(u_signed64 subReqId,
 u_signed64 fileId,
 const char* nsHost,
 int* errorCode,
 char** errorMsg);

/**
 * Informs the stager that an update subrequest has written
 * bytes into a given diskCopy. The diskCopy's status will
 * be updated to STAGEOUT and the other diskcopies of the
 * CastorFile will be invalidated
 * @param subRequestId the id of the SubRequest concerned
 * @param fileId the id of the castorFile
 * @param nsHost the name server hosting this castorFile
 * @param errorCode error code in case of error
 * @param errorMsg error message in case of error. Note that
 * memory is allocated for the message and that the caller is
 * responsible for releasing it
 * @return 0 in case of success, -1 otherwise.
 * If -1, then errorCode and errorMsg are filled.
 */
int Cstager_IJobSvc_firstByteWritten
(u_signed64 subRequestId,
 u_signed64 fileId,
 const char* nsHost,
 int* errorCode,
 char** errorMsg);

#endif /* CASTOR_IJOBSVC_H */
