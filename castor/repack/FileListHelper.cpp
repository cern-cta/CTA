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
 * @(#)$RCSfile: FileListHelper.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2006/01/25 08:24:27 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/


#include <iostream>
#include <sys/types.h>
#include <time.h>
#include "castor/repack/FileListHelper.hpp"

namespace castor {

	namespace repack {


//------------------------------------------------------------------------------
// Constructor 1
//------------------------------------------------------------------------------
FileListHelper::FileListHelper(std::string nameserver){
	m_ns = nameserver;
}


//------------------------------------------------------------------------------
// Constructor 2
//------------------------------------------------------------------------------
FileListHelper::FileListHelper(){
	m_ns = "castorns.cern.ch";
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileListHelper::~FileListHelper()
{
	
}

//------------------------------------------------------------------------------
// getFileList
//------------------------------------------------------------------------------
std::vector<std::string>* FileListHelper::getFileList(
      							castor::repack::RepackSubRequest *subreq) throw()
{
	char path[CA_MAXPATHLEN+1];
	char *server = (char*)m_ns.c_str();
	unsigned long i = 0;
	double vecsize = 0;
	signed64 parent_fileid = -1;

	std::vector<std::string>* filenamelist = new std::vector<std::string>();
	
	vecsize = getFileListSegs(subreq);
	if ( vecsize > 0 )
	{
		stage_trace(2,"Fetching filenames for %f Segments",vecsize);
		castor::dlf::Param params[] =
     		 {castor::dlf::Param("Segments", vecsize),
     		  castor::dlf::Param("DiskSpace", subreq->size())};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 14, 2, params);
		
		for ( i=0; i<vecsize; i++)
		{
			RepackSegment* tmp = subreq->segment().at(i);
			if ( tmp->parent_fileid() == parent_fileid ) continue;
			
			if ( Cns_getpath(server, tmp->parent_fileid(), path) < 0 ) {
					stage_trace (3, "%s\n", sstrerror(serrno));
					castor::exception::Internal ex;
	    			ex.getMessage() << "FileListHelper::getFileList(..):" 
	    							<< sstrerror(serrno) << std::endl;
	    							
	    			castor::dlf::Param params[] =
      				{castor::dlf::Param("Standard Message", sstrerror(ex.code())),
      				 castor::dlf::Param("Precise Message", ex.getMessage().str())};
      				castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 15, 2, params);
	    		    throw ex;
			}
			else {
				filenamelist->push_back(path);
				//stage_trace(3,"%s",path);
				parent_fileid = tmp->parent_fileid();
			}
		}
	}
	
	sort(filenamelist->begin(),filenamelist->end());
	
	std::vector<std::string>::iterator j;
	std::string pathname="";
	for (j=filenamelist->begin(); j!= filenamelist->end(); j++ ){
	
		if ( pathname == (*j) ){
			filenamelist->erase(j);
		}
		else
			pathname = (*j);
	}
	
	return filenamelist;
}


//------------------------------------------------------------------------------
// getFileListSegs
//------------------------------------------------------------------------------
int FileListHelper::getFileListSegs(castor::repack::RepackSubRequest *subreq)
{
	int flags;
	u_signed64 segs_size = 0;
	char path[CA_MAXPATHLEN+1];
	char *server = (char*)m_ns.c_str();
	char *vid = (char*)subreq->vid().c_str();
	

	struct Cns_direntape *dtp;
	Cns_list list;
	signed64 parent_fileid = -1;
	
	if ( subreq != NULL )
	{
		/* the tape check was before ! */
		
		
		flags = CNS_LIST_BEGIN;
		/* all Segments from a tape belong to one Request ! */
		while ((dtp = Cns_listtape (server, vid, flags, &list)) != NULL) {
			RepackSegment* rseg= new RepackSegment();
			rseg->setVid(subreq);
			rseg->setFileid(dtp->fileid);
			rseg->setParent_fileid(dtp->parent_fileid);
			subreq->addSegment(rseg);

			segs_size += dtp->segsize;
			flags = CNS_LIST_CONTINUE;

			//stage_trace(3,"Added Segment %d %d, fileseq : %d",dtp->parent_fileid, dtp->fileid, dtp->fsec, dtp->fseq);
		}
		Cns_listtape (server, vid, CNS_LIST_END, &list);
		stage_trace(2,"Size on disk to be allocated: %d", (segs_size/100000000));
		subreq->setSize(segs_size);

		/* TODO: We want to have a ordered list of segments */
		std::sort(subreq->segment().begin(),subreq->segment().end());

		/*std::vector<RepackSegment*>::iterator i;
		for (i=subreq->segment().begin(); i != subreq->segment().end(); i++ ){
			std::cout << (*i)->parent_fileid() << std::endl;
		}*/
		
		return subreq->segment().size();
	}
	return -1;
}

	} // End namespace repack
}	//End namespace castor
