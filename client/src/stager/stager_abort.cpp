/******************************************************************************
 *                      stager_abort.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003-2007 CERN
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
 * @(#)$RCSfile: stager_abort.c,v $ $Revision: 1.14 $ $Release$ $Date: 2009/01/14 17:33:32 $ $Author: sponcec3 $
 *
 * command line for stager_abort
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <iostream>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include "Castor_limits.h"
#include "serrno.h"
#include "Cgetopt.h"
#include "castor/client/BaseClient.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/stager/NsFileId.hpp"
#include "castor/stager/StageAbortRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/FileResponse.hpp"
#include "stager_client_api.h"
#include "stager_client_api_common.hpp"
#include "stager_client_commandline.h"

static struct Coptions longopts[] =
  {
    {"requestid",     REQUIRED_ARGUMENT,  NULL,      'r'},
    {"fileid",        REQUIRED_ARGUMENT,  NULL,      'M'},
    {"filelist",      REQUIRED_ARGUMENT,  NULL,      'f'},
    {"help",          NO_ARGUMENT,        NULL,      'h'},
    {NULL,            0,                  NULL,        0}
  };


/**
 * Displays usage 
 * @param cmd command name 
 */
void usage(char* cmd) {
  std::cerr << "usage: " << cmd << " "
            << "[-h] -r uuid [-M fileid@nsHost [-M ...]] [-f hsmFileList]"
            << std::endl;
}

void parseFileIdNshost(std::string input, castor::stager::NsFileId &nsFileId)
  throw (castor::exception::Exception) {
  std::string::size_type p = input.find('@', 0);
  if (p == std::string::npos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unable to parse argument '" << input << "'";
    throw e;
  }

  // The fileid should only contain numbers and cannot be empty
  std::string fidStr = input.substr(0, p);
  if ((strspn(fidStr.c_str(), "0123456789") != fidStr.length()) ||
      (fidStr.length() <= 0)) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unable to parse argument '" << input << "'";
    throw e;
  }
  nsFileId.setFileid(atoll(fidStr.c_str()));
  nsFileId.setNsHost(input.substr(p+1,input.length()));
}

void cmd_parse(int argc,
               char *argv[],
               castor::stager::StageAbortRequest &req)
  throw (castor::exception::Exception) {
  bool gotUuid = false;
  Coptind = 1;
  Copterr = 1;
  char c;
  while ((c = Cgetopt_long(argc, argv, "r:M:f:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      {
        castor::stager::NsFileId *nsfileid = new castor::stager::NsFileId();
        parseFileIdNshost(Coptarg, *nsfileid);
        nsfileid->setRequest(&req);
        req.addFiles(nsfileid);
      }
      break;
    case 'f':
      {
	FILE *infile;
	char line[CA_MAXPATHLEN+1];
	infile = fopen(Coptarg, "r");
	if(NULL == infile) {
          castor::exception::NoEntry e;
          e.getMessage() << "Unable to read file " << Coptarg;
          throw e;
        }
	while (fgets(line, sizeof(line), infile) != NULL) {
	  // drop trailing \n
	  while (strlen(line) &&
		 ((line[strlen(line)-1] == '\n') || (line[strlen(line)-1] == '\r'))) {
	    line[strlen(line) - 1] = 0;
	  }
          castor::stager::NsFileId nsfileid;
          parseFileIdNshost(line, nsfileid);
          nsfileid.setRequest(&req);
          req.addFiles(&nsfileid);
	}
      }
      break;
    case 'r':
      if (gotUuid) {
        castor::exception::InvalidArgument e;
        e.getMessage() << "-r option can be given only once";
        throw e;
      }
      gotUuid = true;
      req.setParentUuid(Coptarg);
      break;
    case 'h':
      usage (argv[0]);
      exit (EXIT_FAILURE);
      break;
    default:
      castor::exception::InvalidArgument e;
      e.getMessage() << "Unknown option " << c;
      throw e;
    }
  }
  if (!gotUuid) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "No uuid given";
    throw e;
  }
}

int main(int argc, char *argv[]) {
  // creating empty request
  castor::stager::StageAbortRequest req;

  // Parsing the command line and filling request
  try {
    cmd_parse(argc, argv, req);
  } catch (castor::exception::Exception& e) {
    std::cerr << e.getMessage().str() << std::endl;
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  /* Performing the actual call */
  try {
    // Uses a BaseClient to handle the request
    struct stage_options opts;
    opts.stage_host = NULL;
    opts.service_class = NULL;
    opts.stage_port=0;
    getDefaultForGlobal(&opts.stage_host,&opts.stage_port,&opts.service_class);
    castor::client::BaseClient client(stage_getClientTimeout());
    setDefaultOption(&opts);
    client.setOptions(&opts);
    client.setAuthorizationId();

    // Using the VectorResponseHandler which stores everything in
    // a vector. BEWARE, the responses must be de-allocated afterwards
    std::vector<castor::rh::Response *>respvec;
    castor::client::VectorResponseHandler rh(&respvec);
    std::string reqid = client.sendRequest(&req, &rh);

    // Parsing the responses which have been stored in the vector
    if (respvec.size() <= 0) {
      // We got not replies, this is not normal !
      std::cerr << "No answer received" << std::endl;
      return -1;
    }
    std::cout << "Received " << respvec.size() << " responses" << std::endl;
    for (int i=0; i<(int)respvec.size(); i++) {
      // Casting the response into a FileResponse !
      castor::rh::FileResponse* fr =
        dynamic_cast<castor::rh::FileResponse*>(respvec[i]);
      if (0 == fr) {
        castor::exception::Exception e(SEINTERNAL);
        e.getMessage() << "Error in dynamic cast, response was NOT a file response";
        throw e;
      }
      if (fr->fileId() == 0 && respvec.size() == 1) {
        std::cout << "Error " << fr->errorCode() << " : " << fr->errorMessage() << std::endl;
      } else {
        std::cout << fr->fileId() << " : ";
        if (0 != fr->errorCode()) {
          std::cout << " error " << fr->errorCode() << " : " << fr->errorMessage();
        } else {
          std::cout << "aborted";
        }
        std::cout << std::endl;
      }
    }
  } catch (castor::exception::Exception& e) {
    std::cerr << "Unknown exception call : " << std::endl
              << e.getMessage().str() << std::endl;
  }
}
