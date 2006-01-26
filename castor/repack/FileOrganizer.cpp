/******************************************************************************
 *                      RepackServerReqSvcThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 * @(#)$RCSfile: FileOrganizer.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/01/26 13:55:00 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/
 
#include "FileOrganizer.hpp"
 
namespace castor {
	namespace repack {
		
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileOrganizer::FileOrganizer(){
	
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileOrganizer::~FileOrganizer(){

}
		
FileOrganizer::stage_files(std::vector<std::string*> filenamelist){
	
	
	
	struct stage_prepareToGet_filereq[filenamelist->size()] requests;
	struct stage_prepareToGet_fileresp** resp;
	
	for (i = 0; i < filenamelist->size(); i++) {
        requests[i].filename = filenamelist[i].c_str());
        requests[i].protocol = strdup(req->protocol().c_str());
        // Before calling the stager, set the status of all the subrequests to in progress
        svcs->updateRep(&ad, subreqs[i], true);
      }
	
	stage_prepareToGet(
	
}
		
		
	} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
