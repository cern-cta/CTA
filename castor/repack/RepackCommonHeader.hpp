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

namespace castor{
	namespace repack {
		
		enum RepackConstants {
		CSP_REPACKSERVER_PORT = 62800,
		
		SUBREQUEST_READYFORSTAGING = 1001,
		SUBREQUEST_STAGING		 = 1020,
		SUBREQUEST_READYFORMIG	 = 1030,
		SUBREQUEST_MIGRATING	 = 1040,
		SUBREQUEST_READYFORCLEANUP = 1050,
		SUBREQUEST_DONE			 = 1060
		};
		
		enum RepackServerCodes {
		REPACK = 1,
		REMOVE_TAPE	= 2,
		GET_STATUS = 3	//TODO
		};
	}
}


#endif //REPACKCOMMONHEADER
