/*
 * $Id: stager_client_api_common.cpp,v 1.7 2004/12/06 20:36:28 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_common.cpp,v $ $Revision: 1.7 $ $Date: 2004/12/06 20:36:28 $ CERN IT-ADC/CA Benjamin COuturier";
#endif

/* ============== */
/* System headers */
/* ============== */
#include <stdlib.h>
#include <sstream>

/* ============= */
/* Local headers */
/* ============= */
#include "stager_api.h"
#include "castor/stager/SubRequest.hpp"
#include <serrno.h>


/* ================= */
/* Internal routines */
/* ================= */

/* Routines to free the structures */

int _free_prepareToGet_filereq (struct stage_prepareToGet_filereq  *ptr) { 
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->filename != NULL) free (ptr->filename);
  free(ptr);
  return 0; 
}

int _free_prepareToGet_fileresp (struct stage_prepareToGet_fileresp  *ptr) { 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  free(ptr);
  return 0; 
}

int _free_io_fileresp (struct stage_io_fileresp  *ptr){ 
  if (ptr->castor_filename != NULL) free (ptr->castor_filename);
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->server != NULL) free (ptr->server);
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  free(ptr);
  return 0; 
}
int _free_prepareToPut_filereq (struct stage_prepareToPut_filereq  *ptr){ 
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->filename != NULL) free (ptr->filename);
  free(ptr);
  return 0; 
}
int _free_prepareToPut_fileresp (struct stage_prepareToPut_fileresp  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  free(ptr);
  return 0; 
}

int _free_prepareToUpdate_filereq (struct stage_prepareToUpdate_filereq  *ptr) { 
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->filename != NULL) free (ptr->filename);
  free(ptr);
  return 0; 
}

int _free_prepareToUpdate_fileresp (struct stage_prepareToUpdate_fileresp  *ptr) { 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  free(ptr);
  return 0; 
}



int _free_filereq (struct stage_filereq  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  free(ptr);
  return 0; 
}

int _free_fileresp (struct stage_fileresp  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  free(ptr);
  return 0; 
}

int _free_updateFileStatus_filereq (struct stage_updateFileStatus_filereq  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  free(ptr);
  return 0; 
}

int _free_query_req (struct stage_query_req  *ptr){ 
  if (ptr->param != NULL) free (ptr->param);
  free(ptr);
  return 0; 
}

int _free_filequery_resp (struct stage_filequery_resp  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->poolname != NULL) free (ptr->poolname);
  free(ptr);
  return 0; 
}

int _free_requestquery_resp (struct stage_requestquery_resp  *ptr){ 
  if (ptr->requestId != NULL) free (ptr->requestId);
  free(ptr);
  return 0; 
}

int _free_subrequestquery_resp (struct stage_subrequestquery_resp  *ptr){ 
  free(ptr);
  return 0; 
}

int _free_findrequest_resp (struct stage_findrequest_resp  *ptr){ 
  if (ptr->requestId != NULL) free (ptr->requestId);
  free(ptr);
  return 0; 
}

/* ================= */
/* External routines */
/* ================= */

/* Utility Routines to allocate/delete the lists 
   of structures taken as input by the API functions */

ALLOC_STRUCT_LIST(prepareToGet_filereq)
ALLOC_STRUCT_LIST(prepareToGet_fileresp)
ALLOC_STRUCT_LIST(io_fileresp)
ALLOC_STRUCT_LIST(prepareToPut_filereq)
ALLOC_STRUCT_LIST(prepareToPut_fileresp)
ALLOC_STRUCT_LIST(prepareToUpdate_filereq)
ALLOC_STRUCT_LIST(prepareToUpdate_fileresp)
ALLOC_STRUCT_LIST(filereq)
ALLOC_STRUCT_LIST(fileresp)
ALLOC_STRUCT_LIST(updateFileStatus_filereq)
ALLOC_STRUCT_LIST(query_req)
ALLOC_STRUCT_LIST(filequery_resp)
ALLOC_STRUCT_LIST(requestquery_resp)
ALLOC_STRUCT_LIST(subrequestquery_resp)
ALLOC_STRUCT_LIST(findrequest_resp)


FREE_STRUCT_LIST(prepareToGet_filereq)
FREE_STRUCT_LIST(prepareToGet_fileresp)
FREE_STRUCT_LIST(io_fileresp)
FREE_STRUCT_LIST(prepareToPut_filereq)
FREE_STRUCT_LIST(prepareToPut_fileresp)
FREE_STRUCT_LIST(prepareToUpdate_filereq)
FREE_STRUCT_LIST(prepareToUpdate_fileresp)
FREE_STRUCT_LIST(filereq)
FREE_STRUCT_LIST(fileresp)
FREE_STRUCT_LIST(updateFileStatus_filereq)
FREE_STRUCT_LIST(query_req)
FREE_STRUCT_LIST(filequery_resp)
FREE_STRUCT_LIST(requestquery_resp)
FREE_STRUCT_LIST(subrequestquery_resp)
FREE_STRUCT_LIST(findrequest_resp)




EXTERN_C char *stage_statusName(int statusCode) {
  return (char *)castor::stager::SubRequestStatusCodesStrings[statusCode];
}


EXTERN_C char* DLL_DECL stage_geturl(struct stage_io_fileresp *io) {
  
  char *func = "stage_geturl";
  
  if (io == NULL) {
    serrno = EINVAL;
    stage_errmsg(func, "io is NULL");
    return NULL;
  }
  
  std::stringstream sst;
  
  if (io->protocol != NULL) {
    sst << io->protocol << "://";
  }

  if (io->server != NULL) {
    sst << io->server;
    if (io->port > 0) {
      sst << ":" << std::dec << io->port;;
    }    
    sst << "/";
  }
  
  if (io->filename != NULL) {
    sst << io->filename;
  }
  
  return strdup(sst.str().c_str());
}
