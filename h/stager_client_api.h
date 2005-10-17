/******************************************************************************
 *                      stager_client_api.h
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
 * @(#)$RCSfile: stager_client_api.h,v $ $Revision: 1.24 $ $Release$ $Date: 2005/10/17 12:17:32 $ $Author: jdurand $
 *
 * 
 *
 * @author Ben Couturier
 *****************************************************************************/

/** @file $RCSfile: stager_client_api.h,v $
 * @version $Revision: 1.24 $
 * @date $Date: 2005/10/17 12:17:32 $
 */
/** @mainpage CASTOR New Stager API Proposal
 * $RCSfile: stager_client_api.h,v $ $Revision: 1.24 $
 *
 * @section intro Introduction
 * The new API for the CASTOR stager has been based on the requirements for the 
 * new internal design of the stager. It differs from the previous CASTOR Stager
 * API as we have tried to keep the same names and concepts as in the 
 * Grid Storage Resource Manager (SRM) v2.1.
 * c.f. http://sdm.lbl.gov/srm-wg/
 *
 * @section overview Overview
 *
 * 
 * The stager API allows to contact the CASTOR disk pool manage(stager), so as to
 * put or get file into/from CASTOR. There calls to get files from CASTOR
 * (stage_get, stage_prepareToGet, stage_getNext) or to put files into the system (stage_put,
 * stage_prepareToPut). The other calls deal with file meta-data (stage_putDone,
 * stage_changeFileStatus) or queries.
 * So here's the summary of our last discussion with Olof, regarding these points:
 *
 * 1) Writing files to CASTOR
 *
 * Two calls are available to put files into CASTOR:
 *
 * - stage_prepareToPut which creates the file on CASTOR, and reserves space on a diskserver
 * but does not schedule a mover to access the file. This call allows to create several 
 * files in one go. It is necessary to call stage_put to actually access the data. 
 * Furthermore, when a file is created with this call, it is not migrated automatically,
 * put stage_putDone must be used to make sure that this happens (the stager might howvere have
 * a maximum time for files to stay on disk only, and decide to force a migration).
 *
 * - stage_put which can behave in two ways;
 * Called after stagePrepareToPut, it just schedules access to the file, and the status 
 * stays STAGEOUT when the mover exits (this takes care of use case 3 (see below).
 * Called on its own, it will reserve the space (possibly using defaults values)  
 * and will schedule access to the file. This mode will be used for POSIX file access to CASTOR
 *
 * Concerning concurrency when several users write to a file, we do not guarantee consistency 
 * (we have no such thing as transctions); As a matter of fact, the stagePutDone function doesn't even 
 * need the request id of the prepare to put request, as in the SRM, as this doesn't 
 * bring any more consistency anyway.
 *
 * 2) Getting files from CASTOR
 *
 * Several calls are available to get data from CASTOR.:
 * 
 * - stage _prepareToGet starts recalling the a number of files from tape. It returns immadiately
 * (equivalent to the old stagein --nowait) and does NOT schedule I/O access to the file.
 * It is then necessary to call either stage_get or stage_getNext to access the data.
 * It is possible to prioritize the files in the request by using the priority field in
 * the struct stage_prepareToGet_filerequest. Request with lower priority are processed first,
 *
 * - stage _prepareToUpdate is a cross between prepareToGet and prepareToPut. It recalls the file
 * from tape and allows opening it in write mode (with stage_getNext or stage_update). 
 * To allow the file to be migrated back to tape, stage_putDone must be called.
 * 
 * - stage_get which can behave in two ways;
 * Called after stage_prepareToGet, it just schedules access to the file, blocking
 * while the file is being recalled, until is is finally on disk. The file is opemned in read-only
 * mode.
 *
 * - stage_update which behaves like stage_get except that the file is opened in write mode.
 *
 * - stage_getNext which can be used to process the file brought back from tape
 * by a stage_prepareToGet request, as soon as they are ready to be accessed.
 * This call blocks until the next file is ready to be accessed, the order depending on
 * which files were faster to get from the store (this is optimized by the stager).
 *
 * After the the file have been used, calling stage_releaseFiles on the un-necessary files
 * can help the grabage collection on the stager.
 *
 * @section uc Use Cases for the Stager API
 *
 * 1) User reading/writing a file with rfio_open
 *
 * In this case, the RFIO API does the proper StageGet/StagePut operation
 * according to the mode in which the file is opened. The Stager schedules
 * the request (i.e. space allocation and get from tape if necessary) and
 * then starts the mover. This covers POSIX data access to CASTOR (and CDR as well ?)
 *
 * 2)  User Job doing pre-staging
 *
 * At the beginning of the job, the user calls StageGet with the list of
 * files he is going to use in the job. The StageGet is done with the "no
 * wait" option, and subsequent data access is going to be done using
 * rfio_open (c.f. use case 1.1).
 *
 * 3) Grid user using SRM to copy a file to CASTOR
 *
 * A user first call SrmPrepareToPut (calling ateg_prepareToPut) to create a file
 * in CASTOR. He is returned a tURL (which form is still to decide) that he
 * can use to write to the file using GridFTP.
 * GridFTP opens the file (possibly several times in parallel, if it is a
 * multi-stream transfer) and writes the data.
 * When the transfer is done, the user calls SrmPutDone (which itself calls
 * stage_putDone) specifying the castor filename(s), as well as the request ID.
 *
 * 4) New Stager User using the new API to acess its data.
 *
 * A user calls StageGet on multiple files, which he has to analyze independently.
 * He specifies a callback, which is called by the API for each file as soon as it
 * is staged (no order is guaranteed, the callback is called as soon as the file is
 * available).
 *
 * @section diff_old_stager  Differences with old stager API
 * - The physical pathname of the files is not shown any more. Every file access must 
 *   be duly scheduled using the proper method.
 */

#ifndef stager_client_api_h
#define stager_client_api_h

#include <osdep.h>
#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#endif
#include <sys/types.h>


/** \addtogroup Functions */
/*\@{*/


////////////////////////////////////////////////////////////
//    stager options                                      //
////////////////////////////////////////////////////////////

/**
 * Structure common to calls, containing API options,
 * such as the stage
 * The CASTOR file name must be specified, as well as the mode (RO/RW)
 */
struct stage_options {
  char *stage_host;
  char *service_class;
};



////////////////////////////////////////////////////////////
//    stage_PrepareToGet                                  //
////////////////////////////////////////////////////////////


/**
 * Request structure to get a file from CASTOR.
 * The CASTOR file name must be specified.
 * The priority parameter allows to sort 
 * the files in a stageGet request.
 */
struct stage_prepareToGet_filereq {
  /**
   * String representing the protocol that should be used to access he data (rfio, root ...)
   */
  char          *protocol;

  /**
   * The CASTOR filename of the files to be retrieved from the store.
   */
  char		*filename;

  /**
   * The priority of the file in the request. The stager will start by retieveing all the files
   * with the lowest priority. If the order does not matter set this field to the same value for 
   * all files.
   */
  int           priority;
};

/**
 * Response to a file Request from CASTOR
 */
struct stage_prepareToGet_fileresp {
  /**
   * The CASTOR filename of the file.
   */
  char		*filename;

  /**
   * Size of the file taken from the CASTOR Name Server
   */
  u_signed64	filesize;

  /**
   * Status of the request
   */
  int		status;

  /**
   * Error code
   */
  int errorCode;
  
  /**
   * Error message, if the error code indicates a problem
   */
  char		*errorMessage;
};

/**
 * stage_prepareToGet
 * Stages the files from CASTOR , but does not schedule the file access.
 * \ingroup Functions
 * @param userTag    A string chosen by user to group requests
 * @param requests   Pointer to the flist of file requests
 * @param nbreqs     Number of file requests in the list
 * @param responses  List of file responses, created by the call itself
 * @param nbresps    Number of file responses in the list
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_prepareToGet _PROTO((const char *userTag,
                                                 struct stage_prepareToGet_filereq *requests,
                                                 int nbreqs,
                                                 struct stage_prepareToGet_fileresp **responses,
                                                 int *nbresps,
                                                 char **requestId,
                                                 struct stage_options* opts));



////////////////////////////////////////////////////////////
//    stage_get                                           //
////////////////////////////////////////////////////////////
		       


/**
 * Response to a file Request from CASTOR..
 */
struct stage_io_fileresp {
  /**
   * The CASTOR filename of the file
   */
  char          *castor_filename;

  /**
   * String reprenting the protocol that should be used to access he data (rfio, root ...)
   */
  char		*protocol;

  /**
   * The name of the server which contains the file
   */
  char		*server;

  /**
   * The port on which the data mover is listening
   */
  int   port;

  /**
   * The name of the file on the server. Could be the physical if the protocol is local
   * (e.g. for Storage Tank) or whtever is needed by the protocol.
   */
  char		*filename;

  /**
   * Status of the request
   */
  int		status;

  /**
   * Error code
   */
  int		errorCode;

  /**
   * Error message, if the error code indicates a problem
   */
  char		*errorMessage;

};

		       
/**
 * stage_get
 * Stages one file from CASTOR, and schedules the data access.
 * The file is opened Read only 
 * \ingroup Functions
 * 
 * @param userTag    A string chosen by user to group requests
 * @param protocol   The protocol requested to access the file
 * @param filename   The CASTOR filename
 * @param response   fileresponse structure
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_get _PROTO ((const char *userTag,
                                         const char *protocol,
                                         const char *filename,
                                         struct stage_io_fileresp **response,
                                         char **requestId,
                                         struct stage_options* opts));


////////////////////////////////////////////////////////////
//    stage_getNext                                       //
////////////////////////////////////////////////////////////
		       


/**
 * stage_getNext
 * Schedules access to the next file in the prepateToGet
 * request that is ready to be accessed.
 * \ingroup Functions
 * 
 * @param reqId      ID of the stage_prepareToGetRequest
 * @param response   The location of the file
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int stage_getNext _PROTO((const char *reqId,
                                   struct stage_io_fileresp ** response,
                                   struct stage_options* opts));



////////////////////////////////////////////////////////////
//    stage_prepareToUpdate                               //
////////////////////////////////////////////////////////////

/**
 * Request structure to update a file from CASTOR.
 * The CASTOR file name must be specified.
 * The priority parameter allows to sort 
 * the files in a stageUpdate request.
 */
struct stage_prepareToUpdate_filereq {
  /**
   * String representing the protocol that should be used to access he data (rfio, root ...)
   */
  char          *protocol;

  /**
   * The CASTOR filename of the files to be retrieved from the store.
   */
  char		*filename;

  /**
   * The priority of the file in the request. The stager will start by retieveing all the files
   * with the lowest priority. If the order does not matter set this field to the same value for 
   * all files.
   */
  int           priority;
};

/**
 * Response to a file Request from CASTOR
 */
struct stage_prepareToUpdate_fileresp {
  /**
   * The CASTOR filename of the file.
   */
  char		*filename;

  /**
   * Size of the file taken from the CASTOR Name Server
   */
  u_signed64	filesize;

  /**
   * Status of the request
   */
  int		status;

  /**
   * Error code
   */
  int errorCode;
  
  /**
   * Error message, if the error code indicates a problem
   */
  char		*errorMessage;
};



/**
 * stage_prepareToUpdate
 * Stages in the files from CASTOR, on disk servers where they will; be editable,  but does not 
 * schedule the file access
 * \ingroup Functions
 * @param userTag    A string chosen by user to group requests
 * @param requests   Pointer to the flist of file requests
 * @param nbreqs     Number of file requests in the list
 * @param responses  List of file responses, created by the call itself
 * @param nbresps    Number of file responses in the list
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_prepareToUpdate _PROTO((const char *userTag,
                                                    struct stage_prepareToUpdate_filereq *requests,
                                                    int nbreqs,
                                                    struct stage_prepareToUpdate_fileresp **responses,
                                                    int *nbresps,
                                                    char **requestId,
                                                    struct stage_options* opts));


////////////////////////////////////////////////////////////
//    stage_update                                        //
////////////////////////////////////////////////////////////


/**
 * stage_update
 * Stages one file from CASTOR, and schedules the data access.
 * the file is opened in read-write mode.
 * \ingroup Functions
 * 
 * @param userTag    A string chosen by user to group requests
 * @param protocol   The protocol requested to access the file
 * @param filename   The CASTOR filename
 * @param create     If set to one, the file will be created if it does not exist
 * @param mode       The mode bits for the file when created
 * @param size       The expected final size of the file, or 0 if not known
 * @param response   fileresponse structure
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_update _PROTO ((const char *userTag,
                                            const char *protocol,
                                            const char *filename,
                                            int    create,
                                            mode_t mode,
					    u_signed64 size,
                                            struct stage_io_fileresp **response,
                                            char **requestId,
                                            struct stage_options* opts));


////////////////////////////////////////////////////////////
//    stage_updateNext                                    //
////////////////////////////////////////////////////////////
		       


/**
 * stage_updateNext
 * Schedules access to the next file in the prepateToUpdate
 * request that is ready to be accessed.
 * \ingroup Functions
 * 
 * @param reqId      ID of the stage_prepareToUpdateRequest
 * @param response   The location of the file
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int stage_updateNext _PROTO((const char *reqId,
                                      struct stage_io_fileresp ** response,
                                      struct stage_options* opts));





////////////////////////////////////////////////////////////
//    stage_PrepareToPut                                  //
////////////////////////////////////////////////////////////


/**
 * Request structure to put a file to CASTOR.
 */
struct stage_prepareToPut_filereq {

  /**  
   * String representing the protocol that should be used to access he data (rfio, root ...)
   */ 
  char          *protocol;

  /**
   * The CASTOR filename of the files to be retrieved from the store.
   */
  char		*filename;

  /**
   * Mode for opening the file
   */
  int		mode;

  /**
   * Size
   */
  u_signed64	filesize;

};

/**
 * Response to a prepareToPut file Request.
 */
struct stage_prepareToPut_fileresp {
  /**
   * Name of the file
   */
  char		*filename;

  /**
   * Size
   */
  u_signed64	filesize;

  /**
   * Status of the sub request concerning the file
   */
  int		status;

  /**
   * Error code
   */
  int		errorCode;

  /**
   * Error message, if the error code indicates a problem
   */
  char		*errorMessage;

};

/**
 * stage_prepareToPut
 * Reserve space so as to put files in CASTOR, but do not schedule access 
 * to those files. The files created with stage_PrepareToPut must be validated
 * once they are ready to be migrated by the stage_putDone call.
 * \ingroup Functions
 * 
 * @param userTag    A string chosen by user to group requests
 * @param requests   Pointer to the list of file requests
 * @param nbreqs     Number of file requests in the list
 * @param responses  List of file responses, created by the call itself
 * @param nbresps    Number of file responses in the list
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_prepareToPut _PROTO ((const char *userTag,
                                                  struct stage_prepareToPut_filereq *requests,
                                                  int nbreqs,
                                                  struct stage_prepareToPut_fileresp **responses,
                                                  int *nbresps,
                                                  char **requestId,
                                                  struct stage_options* opts));

////////////////////////////////////////////////////////////
//    stage_put                                           //
////////////////////////////////////////////////////////////
		       
/**
 * stage_put
 * Stages one file into CASTOR, and schedules the data access.
 * \ingroup Functions
 * 
 * @param userTag    A string chosen by user to group requests
 * @param protocol   The protocol requested to access the file
 * @param filename   The CASTOR filename
 * @param mode       The mode in which the file is to be opened
 * @param size       The expected filesize of the file that is going to be
 *                   writen (or 0, in which case the stager will take
 *                   its default)
 * @param response   fileresponse structure
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_put _PROTO((const char *userTag,
                                        const char *protocol,
                                        const char *filename,
                                        mode_t mode,
					u_signed64 size,
					struct stage_io_fileresp **response,
                                        char **requestId,
                                        struct stage_options* opts));



////////////////////////////////////////////////////////////
//    stage_putNext                                       //
////////////////////////////////////////////////////////////
		       


/**
 * stage_putNext
 * Schedules access to the next file in the prepateToPut
 * request that is ready to be accessed.
 * \ingroup Functions
 * 
 * @param reqId      ID of the stage_prepareToPutRequest
 * @param response   The location of the file
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int stage_putNext _PROTO((const char *reqId,
                                   struct stage_io_fileresp ** response,
                                   struct stage_options* opts));



////////////////////////////////////////////////////////////
//    stage_putDone                                       //
////////////////////////////////////////////////////////////


/**
 * Generic structure containing a CASTOR filename
 */
struct stage_filereq {
  /**
   * The name of the CASTOR file on which the put is done
   */
  char		*filename;
};

/**
 * Response to a stage_filereq.
 */
struct stage_fileresp {
  /**
   * The CASTOR filename
   */
  char		*filename;

  /**
   * Status of the request
   */
  int		status;

  /**
   * Error code
   */
  int		errorCode;

  /**
   * Error message, if the error code indicates a problem
   */
  char		*errorMessage;


};

/**
 * stage_putDone
 * Changes the status of the files, indicating that the put request is 
 * successfully done. This call is neccessary when stage_prepareToPut
 * has been called.
 * \ingroup Functions
 *
 * 
 * @param putRequestId  ID of the related prepare to put request 
 * @param requests   Pointer to the list of file requests
 * @param nbreqs     Number of file requests in the list
 * @param responses  List of file responses, created by the call itself
 * @param nbresps    Number of file responses in the list
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_putDone _PROTO((char *putRequestId,
					    struct stage_filereq *requests,
                                            int nbreqs,
                                            struct stage_fileresp **responses,
                                            int *nbresps,
                                            char **requestId,
                                            struct stage_options* opts));



////////////////////////////////////////////////////////////
//    stage_rm                                   //
////////////////////////////////////////////////////////////

/**
 * stage_rm
 * Clears the files from the stager.
 * \ingroup Functions
 * 
 * @param requests   Pointer to the list of file requests
 * @param nbreqs     Number of file requests in the list
 * @param responses  List of file responses, created by the call itself
 * @param nbresps    Number of file responses in the list
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_rm _PROTO ((struct stage_filereq *requests,
                                        int nbreqs,
                                        struct stage_fileresp **responses,
                                        int *nbresps,
                                        char **requestId,
                                        struct stage_options* opts));

////////////////////////////////////////////////////////////
//    stage_releaseFiles                                  //
////////////////////////////////////////////////////////////



/**
 * stage_releaseFiles
 * Indicates to the stager that the user has finished with the files
 * and that they can garbage collected.
 * \ingroup Functions
 * 
 * @param requests   Pointer to the list of file requests
 * @param nbreqs     Number of file requests in the list
 * @param responses  List of file responses, created by the call itself
 * @param nbresps    Number of file responses in the list
 * @param requestId  Reference number to be used by the client to look
 *                   up his request in the castor stager.
 * @param opts       CASTOR stager specific options
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_releaseFiles _PROTO((struct stage_filereq *requests,
                                                 int nbreqs,
                                                 struct stage_fileresp **responses,
                                                 int *nbresps,
                                                 char **requestId,
                                                 struct stage_options* opts));


////////////////////////////////////////////////////////////
//    stage_abortRequest                                  //
////////////////////////////////////////////////////////////


/**
 * stage_abortRequest
 * Aborts an aynchronous running request, either a prepareToPut or
 * a prepareToGet request.
 * \ingroup Functions
 * 
 * @param requestId  The request to be stopped.
 * @param opts       CASTOR stager specific options
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int DLL_DECL stage_abortRequest _PROTO((char *requestId,
                                                 struct stage_options* opts));



////////////////////////////////////////////////////////////
//    stage_filequery                                     //
////////////////////////////////////////////////////////////

enum query_type { BY_FILENAME, BY_REQID, BY_USERTAG, BY_FILEID };


/**
 * Query request input structure
 */
struct stage_query_req {
  /**
   * Type of the query
   */
   int type;
  
   /**
    * Pointer to value
    */
   void *param;
 };

/**
 * Request structure to put a file to CASTOR.
 */
struct stage_filequery_resp {

  /**
   * The name of the CASTOR file on which the put is done
   */
  char		*filename;

  /**
   * The CASTOR file id
   */
  u_signed64    fileid;

  /**
   * The status for the file
   */
  int    status;

  /**
   * The size of the file
   */
  u_signed64    size;

  /**
   * The server having a disk copy
   */
  char *diskserver;

  /**
   * The pool containing the file
   */
  char *poolname;

  /**
   * The file creation time
   */
  TIME_T creationTime;

  /**
   * Last access time
   */
  TIME_T accessTime;

  /**
   * Total number of accesses for the file
   */ 
  int nbAccesses;

 /**
   * Error code
   */
  int		errorCode;

  /**
   * Error message, if the error code indicates a problem
   */
  char		*errorMessage;

};



/**
 * stage_filequery
 * Returns summary information about the files in the CASTOR stager.
 * \ingroup Functions
 * 
 * 
 * @param requests   Pointer to the list of file requests
 * @param nbreqs     Number of file requests in the list
 * @param responses  List of file responses, created by the call itself
 * @param nbresps    Number of file responses in the list
 * @param opts       CASTOR stager specific options
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_filequery _PROTO((struct stage_query_req *requests,
                                              int nbreqs,
                                              struct stage_filequery_resp **responses,
                                              int *nbresps,
                                              struct stage_options* opts));


////////////////////////////////////////////////////////////
//    stage_requestquery                                  //
////////////////////////////////////////////////////////////


/**
 * Response to a request query
 */
struct stage_requestquery_resp {
  /**
   * The request ID
   */
  char		*requestId;

  /**
   * The status for the request
   */
  int    status;

  /**
   * The request creation time
   */
  TIME_T creationTime;

  /**
   * Last modification time
   */
  TIME_T modificationTime;

  /**
   * Array of stage_subrequestquery_resp giving the status of all subrequests
   */
  struct stage_subrequestquery_resp *subrequests;

  /**
   *  Number of subrequests in the array
   */                                                 
  int nbsubrequests;

 /**
   * Error code
   */
  int		errorCode;

  /**
   * Error message, if the error code indicates a problem
   */
  char		*errorMessage;

};


/**
 * Subrequest response
 */
struct stage_subrequestquery_resp {

  /**
   * The status for the sub-request
   */
  int    status;

  /**
   * Last modification time
   */
  TIME_T modificationTime;

};

/**
 * stage_query
 * Returns information about CASTOR stage requests
 * \ingroup Functions
 * 
 * 
 * @param requests   Pointer to the list of requests
 * @param nbreqs     Number of requests in the list
 * @param responses  List of responses, created by the call itself
 * @param nbresps    Number of responses in the list
 * @param subresponses  List of sub responses, created by the call itself
 * @param nbsubresps    Number of sub-responses in the list
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_requestquery _PROTO((struct stage_query_req *requests,
                                                 int nbreqs,
                                                 struct stage_requestquery_resp **responses,
                                                 int *nbresps,
                                                 struct stage_options* opts));



////////////////////////////////////////////////////////////
//    stage_findrequest                                   //
////////////////////////////////////////////////////////////


/**
 * Request structure to put a file to CASTOR.
 */
struct stage_findrequest_resp {

  /**
   * The id of the matching request
   */
  char *requestId;

};



/**
 * stage_findrequest
 * Find requests dealing with given file gives or usertags.
 * \ingroup Functions
 * 
 * 
 * @param requests   Pointer to the list of file requests
 * @param nbreqs     Number of file requests in the list
 * @param responses  List of file responses, created by the call itself
 * @param nbresps    Number of file responses in the list
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 * @note requestId and responses are allocated by the call, and therefore
 *       should be freed by the client.
 */
EXTERN_C int DLL_DECL stage_findrequest _PROTO((struct stage_query_req *requests,
                                                int nbreqs,
                                                struct stage_findrequest_resp **responses,
                                                int *nbresps,
                                                struct stage_options* opts));


////////////////////////////////////////////////////////////
//    stage_seterrbuf                                     //
////////////////////////////////////////////////////////////

/**
 * stage_seterrbuf
 * Sets the error buffer in which the detail of the error messages
 * is copied.
 * \ingroup Functions
 * 
 * @param buffer     Pointer buffer where the error strings should be copied
 * @param buflen     Actual length of the buffer
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int DLL_DECL stage_seterrbuf _PROTO((char *buffer, int buflen));




////////////////////////////////////////////////////////////
//    stage_open                                          //
////////////////////////////////////////////////////////////

/**
 * stage_open
 * Wrapper function that takes stadand POSIX flags, and calls the proper
 * stage API function to access the files accordingly
 * \ingroup Functions
 * 
 * @param userTag    A string chosen by user to group requests
 * @param protocol   The protocol requested to access the file
 * @param filename   The CASTOR filename
 * @param flags      POSIX flags for opening the file
 * @param mode       The mode bits for the file when created
 * @param response   fileresponse structure
 * @param requestId  Reference number to be used by the client to look
 *                   up his request1 in the castor stager.
 * @param opts       CASTOR stager specific options 
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int DLL_DECL stage_open _PROTO ((const char *userTag,
                                          const char *protocol,
                                          const char *filename,
                                          int flags,
                                          mode_t mode,
					  u_signed64 size,
                                          struct stage_io_fileresp **response,
                                          char **requestId,
                                          struct stage_options* opts));

////////////////////////////////////////////////////////////
//    stage_geturl                                        //
////////////////////////////////////////////////////////////

/**
 * stage_geturl
 * Parses the stage_io_response to return the URL accordingly
 * \ingroup Functions
 * 
 * @param io   pointer to io response structure
 *
 * @returns The alloced URL, NULL otherwise
 */
EXTERN_C char* DLL_DECL stage_geturl _PROTO ((struct stage_io_fileresp *io));







////////////////////////////////////////////////////////////
//    Utility to display status string                    //
////////////////////////////////////////////////////////////

/**
 * stage_statusName
 * Returns the name of a request status
 * \ingroup Functions
 * 
 * @param statusCode the code of the request
 *
 * @returns The status name as char*
 *
 * @deprecated Use stage_requestStatusName instead
 */
EXTERN_C char* DLL_DECL stage_statusName _PROTO((int statusCode));


/**
 * stage_requestStatusName
 * Returns the name of a request status
 * \ingroup Functions
 * 
 * @param statusCode the code of the request
 *
 * @returns The status name as char*
 */
EXTERN_C char* DLL_DECL stage_requestStatusName _PROTO((int statusCode));

/**
 * Possible file status codes
 */
enum stage_fileStatus {
  FILE_INVALID_STATUS=0,
  FILE_STAGEOUT=1,
  FILE_STAGEIN=2,
  FILE_STAGED=3,
  FILE_CANBEMIGR=4,
  FILE_WAITINGMIGR=5,
  FILE_BEINGMIGR=6,
  FILE_PUTFAILED=7
};

/**
 * stage_fileStatusName
 * Returns the name of a file status
 * \ingroup Functions
 * 
 * @param statusCode the code of the request
 *
 * @returns The status name as char*
 */
EXTERN_C char* DLL_DECL stage_fileStatusName _PROTO((int statusCode));


////////////////////////////////////////////////////////////
//    Utility to get the current client timeout           //
////////////////////////////////////////////////////////////
#define STAGER_TIMEOUT_DEFAULT 2592000

EXTERN_C int DLL_DECL stage_getClientTimeout _PROTO(());




////////////////////////////////////////////////////////////
//    MACROS                                              //
////////////////////////////////////////////////////////////

/**
 * Macro to create a function that allocates a list of STRCNAME structures
 */
#define ALLOC_STRUCT_LIST(STRCNAME)                    \
  EXTERN_C int create_##STRCNAME(struct stage_##STRCNAME **ptr, int nb) {       \
  struct stage_##STRCNAME *ptrlist;                                    \
  if (ptr == NULL || nb <=0) return -1; \
  ptrlist = (struct stage_##STRCNAME *)calloc(nb, sizeof(struct stage_##STRCNAME)); \
  if (ptrlist < 0) return -1; \
  *ptr = ptrlist; \
  return 0; \
}

/**
 * Macro to create a function that frees a list of STRCNAME structures
 */
#define FREE_STRUCT_LIST(STRCNAME)                      \
  EXTERN_C int free_##STRCNAME(struct stage_##STRCNAME *ptr, int nb) {	\
  int i;                                                \
  if (ptr == NULL || nb <=0) return -1;                 \
  for (i=0; i<nb; i++) {                                \
    _free_##STRCNAME(&(ptr[i]));			\
  }							\
  free(ptr);  \
  return 0;   \
}

/**
 * Macro to declare a function that allocates a list of STRCNAME structures
 */
#define ALLOC_STRUCT_LIST_DECL(STRCNAME)                    \
  EXTERN_C int create_##STRCNAME(struct stage_##STRCNAME **ptr, int nb);

/**
 * Macro to declare a function that frees a list of STRCNAME structures
 */
#define FREE_STRUCT_LIST_DECL(STRCNAME)                      \
  EXTERN_C int free_##STRCNAME(struct stage_##STRCNAME *ptr, int nb);


ALLOC_STRUCT_LIST_DECL(prepareToGet_filereq)
ALLOC_STRUCT_LIST_DECL(prepareToGet_fileresp)
ALLOC_STRUCT_LIST_DECL(io_fileresp)
ALLOC_STRUCT_LIST_DECL(prepareToPut_filereq)
ALLOC_STRUCT_LIST_DECL(prepareToPut_fileresp)
ALLOC_STRUCT_LIST_DECL(prepareToUpdate_filereq)
ALLOC_STRUCT_LIST_DECL(prepareToUpdate_fileresp)
ALLOC_STRUCT_LIST_DECL(filereq)
ALLOC_STRUCT_LIST_DECL(fileresp)
ALLOC_STRUCT_LIST_DECL(query_req)
ALLOC_STRUCT_LIST_DECL(filequery_resp)
ALLOC_STRUCT_LIST_DECL(requestquery_resp)
ALLOC_STRUCT_LIST_DECL(subrequestquery_resp)
ALLOC_STRUCT_LIST_DECL(findrequest_resp)


FREE_STRUCT_LIST_DECL(prepareToGet_filereq)
FREE_STRUCT_LIST_DECL(prepareToGet_fileresp)
FREE_STRUCT_LIST_DECL(io_fileresp)
FREE_STRUCT_LIST_DECL(prepareToPut_filereq)
FREE_STRUCT_LIST_DECL(prepareToPut_fileresp)
FREE_STRUCT_LIST_DECL(prepareToUpdate_filereq)
FREE_STRUCT_LIST_DECL(prepareToUpdate_fileresp)
FREE_STRUCT_LIST_DECL(filereq)
FREE_STRUCT_LIST_DECL(fileresp)
FREE_STRUCT_LIST_DECL(query_req)
FREE_STRUCT_LIST_DECL(filequery_resp)
FREE_STRUCT_LIST_DECL(requestquery_resp)
FREE_STRUCT_LIST_DECL(subrequestquery_resp)
FREE_STRUCT_LIST_DECL(findrequest_resp)



////////////////////////////////////////////////////////////
//    Known MOVER protocols                               //
////////////////////////////////////////////////////////////

#define MOVER_PROTOCOL_RFIO "rfio"
#define MOVER_PROTOCOL_ROOT "root"




// XXX Add FindRequest
/*\@}*/

#endif /* stager_client_api_h */
