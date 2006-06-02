#ifndef REPACKCOMMONHEADER
#define REPACKCOMMONHEADER

#include <iostream>
#include <string>
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "h/stager_client_api_common.h"   // for stage_trace("..")
#include "castor/dlf/Dlf.hpp"
#include "castor/Constants.hpp"
#include "RepackRequest.hpp"
#include "RepackSubRequest.hpp"
#include "RepackSegment.hpp"
#include "RepackServer.hpp"
#include "Tools.hpp"

namespace castor{
  namespace repack {

    enum RepackConstants {
    CSP_REPACKSERVER_PORT = 62800,

    SUBREQUEST_READYFORSTAGING = 1,
    SUBREQUEST_STAGING		     = 2,
    SUBREQUEST_MIGRATING	     = 3,
    SUBREQUEST_READYFORCLEANUP = 4,
    SUBREQUEST_DONE            = 5,
    SUBREQUEST_ARCHIVED        = 6
    };

    enum RepackServerCodes {
    REPACK              = 2,
    REMOVE_TAPE	        = 4,
    GET_STATUS          = 6,
    GET_STATUS_ALL      = 8,
    GET_STATUS_ARCHIVED = 10,   // TODO
    RESTART_REPACK      = 12,   // TODO
    ARCHIVE             = 14
    };
	}
}


#endif //REPACKCOMMONHEADER
