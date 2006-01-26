#ifndef REPACKCOMMONHEADER
#define REPACKCOMMONHEADER

#include <iostream>
#include <string>
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "h/stager_client_api_common.h"   // for stage_trace("..")
#include "castor/dlf/Dlf.hpp"
#include "castor/Constants.hpp"
#include "RepackRequest.hpp"
#include "RepackSubRequest.hpp"
#include "RepackSegment.hpp"


#define CSP_REPACKSERVER_PORT 62800

#define SUBREQUEST_READYFORSTAGING	1001
#define SUBREQUEST_STAGING			1020
#define SUBREQUEST_READYFORMIG		1030
#define SUBREQUEST_MIGRATING		1040
#define SUBREQUEST_READYFORCLEANUP	1050
#define SUBREQUEST_DONE				1060


void markStatus(int status, castor::repack::RepackSubRequest* sreq){
	sreq->status = status;
}


#endif //REPACKCOMMONHEADER
