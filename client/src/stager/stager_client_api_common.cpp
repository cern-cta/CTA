/*
 * $Id: stager_client_api_common.cpp,v 1.2 2004/11/19 18:31:31 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_common.cpp,v $ $Revision: 1.2 $ $Date: 2004/11/19 18:31:31 $ CERN IT-ADC/CA Benjamin COuturier";
#endif

/* ============== */
/* System headers */
/* ============== */
#include <stdlib.h>

/* ============= */
/* Local headers */
/* ============= */
#include "stager_client_api.h"

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
  if (ptr->errstring != NULL) free (ptr->errstring);
  free(ptr);
  return 0; 
}

int _free_get_fileresp (struct stage_get_fileresp  *ptr){ 
  if (ptr->castor_filename != NULL) free (ptr->castor_filename);
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->server != NULL) free (ptr->server);
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errstring != NULL) free (ptr->errstring);
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
  if (ptr->errstring != NULL) free (ptr->errstring);
  free(ptr);
  return 0; 
}
int _free_put_fileresp (struct stage_put_fileresp  *ptr){ 
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->server != NULL) free (ptr->server);
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errstring != NULL) free (ptr->errstring);
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
ALLOC_STRUCT_LIST(get_fileresp)
ALLOC_STRUCT_LIST(prepareToPut_filereq)
ALLOC_STRUCT_LIST(prepareToPut_fileresp)
ALLOC_STRUCT_LIST(put_fileresp)
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
FREE_STRUCT_LIST(get_fileresp)
FREE_STRUCT_LIST(prepareToPut_filereq)
FREE_STRUCT_LIST(prepareToPut_fileresp)
FREE_STRUCT_LIST(put_fileresp)
FREE_STRUCT_LIST(filereq)
FREE_STRUCT_LIST(fileresp)
FREE_STRUCT_LIST(updateFileStatus_filereq)
FREE_STRUCT_LIST(query_req)
FREE_STRUCT_LIST(filequery_resp)
FREE_STRUCT_LIST(requestquery_resp)
FREE_STRUCT_LIST(subrequestquery_resp)
FREE_STRUCT_LIST(findrequest_resp)
