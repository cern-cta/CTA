/*
 * $Id: stager_client_api_query.cpp,v 1.1 2004/11/04 19:07:28 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_query.cpp,v $ $Revision: 1.1 $ $Date: 2004/11/04 19:07:28 $ CERN IT-ADC/CA Benjamin Couturier";
#endif

/* ============== */
/* System headers */
/* ============== */

/* ============= */
/* Local headers */
/* ============= */
#include "stager_client_api.h"
#include "stager_admin_api.h"
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/StageGetRequest.hpp"
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "castor/stager/SubRequest.hpp"

/* ================= */
/* External routines */
/* ================= */



////////////////////////////////////////////////////////////
//    stage_filequery                                     //
////////////////////////////////////////////////////////////

EXTERN_C int DLL_DECL stage_filequery(struct stage_query_req **requests,
				      int nbreqs,
				      struct stage_filequery_resp ***responses,
				      int *nbresps){

}


////////////////////////////////////////////////////////////
//    stage_requestquery                                  //
////////////////////////////////////////////////////////////

EXTERN_C int DLL_DECL stage_requestquery(struct stage_query_req **requests,
					 int nbreqs,
					 struct stage_requestquery_resp ***responses,
					 int *nbresps,
					 struct stage_subrequestquery_resp ***subresponses,
					 int *nbsubresps) {

}



////////////////////////////////////////////////////////////
//    stage_findrequest                                   //
////////////////////////////////////////////////////////////


EXTERN_C int DLL_DECL stage_findrequest(struct stage_query_req **requests,
					int nbreqs,
					struct stage_findrequest_resp ***responses,
					int *nbresps){

}

