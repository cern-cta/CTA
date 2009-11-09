/******************************************************************************************************************/
/* Helper class containing the objects and methods which interact to performe the processing of the request      */
/* it is needed to provide:                                                                                     */
/*     - a common place where its objects can communicate                                                      */
/*     - a way to pass the set of objects from the main flow (DBService thread) to the specific handler */
/* It is an attribute for all the request handlers                                                           */
/* **********************************************************************************************************/


#ifndef STAGER_REQUEST_HELPER_HPP
#define STAGER_REQUEST_HELPER_HPP 1

#include "castor/stager/daemon/CnsHelper.hpp"

#include "castor/IObject.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"
#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"


#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/exception/Exception.hpp"

#include "castor/ObjectSet.hpp"
#include "castor/Constants.hpp"

#include "serrno.h"
#include <errno.h>

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"

#include <vector>
#include <iostream>
#include <string>
#include <string.h>
#include <sys/time.h>

namespace castor{
  namespace stager{
    namespace daemon{

      class RequestHelper : public virtual castor::BaseObject{

      public:

	/* services needed: database and stager services*/
	castor::stager::IStagerSvc* stagerService;
	castor::db::DbCnvSvc* dbSvc;

	// for logging purposes
	struct Cns_fileid cnsFileId;

	/* BaseAddress */
	castor::BaseAddress* baseAddr;

	/* subrequest and fileRequest  */
	castor::stager::SubRequest* subrequest;

	castor::stager::FileRequest* fileRequest;

	/* service class */
	castor::stager::SvcClass* svcClass;

	/* castorFile attached to the subrequest*/
	castor::stager::CastorFile* castorFile;

	std::string username;
	std::string groupname;

	/* Cuuid_t thread safe variables */
	Cuuid_t subrequestUuid;
	Cuuid_t requestUuid;

	std::string default_protocol;

	timeval tvStart;

	RequestHelper(castor::stager::SubRequest* subRequestToProcess, int &typeRequest) throw(castor::exception::Exception);

	/* destructor */
	~RequestHelper() throw();

	/**
   * resolve the svClass if not resolved yet by using the stagerService
   */
	void resolveSvcClass() throw(castor::exception::Exception);

	/* get subrequest and request uuids */
	void setUuids() throw ();

	/**
	 *  link the castorFile to the ServiceClass
	 */
	void getCastorFileFromSvcClass(castor::stager::daemon::CnsHelper* stgCnsHelper) throw(castor::exception::Exception);

	/************************************************************************************/
	/* set the username and groupname string versions using id2name c function  */
	/**********************************************************************************/
	void setUsernameAndGroupname() throw(castor::exception::Exception);

	/**
	 * Checks if the user (euid,egid) has the right permission for this request.
	 * Write permissions are needed for any request that changes any attribute of the file,
	 * this includes PutDone.
	 * @param fileCreated true if the file has just been created. In such a case, read permission
	 * is sufficient for any operation, allowing for putting read-only files.
	 * @param stgCnsHelper information about the file to be checked e.g. the uid and gid.
	 * @throw exception when the user has not enough permissions for this request.
	 */
	void checkFilePermission(bool fileCreated,
				 castor::stager::daemon::CnsHelper* stgCnsHelper)
	  throw(castor::exception::Exception);

	/**
	 * Logs a standard message to DLF including all needed info (e.g. filename, svcClass, etc.)
	 * @param level the DLF logging level
	 * @param messageNb the message number as defined in DlfMessages.hpp
	 * @param fid the fileId structure if needed
	 */
	void logToDlf(int level, int messageNb, struct Cns_fileid* fid = 0) throw();

      }; //end RequestHelper class
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor



#endif
