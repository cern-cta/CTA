/*
 * $Id: ErrorSvcThread.cpp,v 1.1 2005/04/18 09:08:03 jdurand Exp $
 */

/*
 * Copyright (C) 2005 by CERN/IT/FIO/DS
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: ErrorSvcThread.cpp,v $ $Revision: 1.1 $ $Date: 2005/04/18 09:08:03 $ CERN IT-FIO/DS Sebastien Ponce";
#endif

/* ================================================================= */
/* Local headers for threads : to be included before ANYTHING else   */
/* ================================================================= */
#include "Cthread_api.h"
#include "Cmutex.h"
#include "stager_uuid.h"                /* Thread-specific uuids */

/* ============== */
/* System headers */
/* ============== */
#include <vector>

/* ============= */
/* Local headers */
/* ============= */
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/FilesDeleted.hpp"
#include "castor/stager/GCRemovedFile.hpp"
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "castor/rh/GCFilesResponse.hpp"
#include "castor/rh/BasicResponse.hpp"
#undef logfunc

#include "stager_service_helper.hpp"
#include "stager_error_service.h"
#include "stager_macros.h"
#include "serrno.h"

#undef NULL
#define NULL 0

/* ---------------------------------------- */
/* stager_error_select()                    */
/*                                          */
/* Purpose: ERROR 'select' part of service  */
/*                                          */
/* Input:  n/a                              */
/*                                          */
/* Output:  (void **) output   Selected     */
/*                                          */
/* Return: 0 [OK] or -1 [ERROR, serrno]     */
/* ---------------------------------------- */

EXTERN_C int DLL_DECL stager_error_select(void **output) {
  char *func =  "stager_error_select";
  int rc;

  STAGER_LOG_ENTER();

  if (output == NULL) {
    serrno = EFAULT;
    return -1;
  }

  castor::stager::Request* req = 0;
  castor::Services *svcs;
  castor::stager::IStagerSvc *stgSvc;

  try {

    /* Loading services */
    /* ---------------- */
    STAGER_LOG_VERBOSE(NULL,"Loading services");
    svcs = castor::BaseObject::services();
    castor::IService* svc =
      svcs->service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
    stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);


    /* Get any new request to do    */
    /* ---------------------------- */
    STAGER_LOG_VERBOSE(NULL,"Getting any request to do");
    std::vector<castor::ObjectsIds> types;
    /* TODO: OBJ_FilesFailed */
    /* types.push_back(castor::OBJ_FilesFailed); */
    /* castor::stager::Request* req = stgSvc->requestToDo(types); */

    if (0 == req) {
      /* Nothing to do */
      STAGER_LOG_VERBOSE(NULL,"Nothing to do");
      serrno = ENOENT;
      rc = -1;
    } else {
      /* Set uuid for the log */
      /* -------------------- */
      Cuuid_t request_uuid;
      if (string2Cuuid(&request_uuid,(char *) req->reqId().c_str()) == 0) {
        stager_request_uuid = request_uuid;
      }
      std::stringstream msg;
      msg << "Found request " << req->id();
      STAGER_LOG_DEBUG(NULL,msg.str().c_str());
      *output = req;
      rc = 0;

    }

  } catch (castor::exception::Exception e) {
    serrno = e.code();
    STAGER_LOG_DB_ERROR(NULL,"stager_error_select",
                        e.getMessage().str().c_str());
    rc = -1;
    if (req) delete req;
  }

  // Cleanup
  stgSvc->release();

  // Return
  STAGER_LOG_RETURN(rc);
}


/* ----------------------------------------- */
/* stager_error_process()                    */
/*                                           */
/* Purpose: Error 'process' part of service  */
/*                                           */
/* Input:  (void *) output    Selection      */
/*                                           */
/* Output: n/a                               */
/*                                           */
/* Return: 0 [OK] or -1 [ERROR, serrno]      */
/* ----------------------------------------- */
EXTERN_C int DLL_DECL stager_error_process(void *output) {

  char *func =  "stager_error_process";
  STAGER_LOG_ENTER();

  serrno = 0;
  if (output == NULL) {
    serrno = EFAULT;
    return -1;
  }

  /* TODO */
  return 0;
}
