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
 * @(#)$RCSfile: FileListHelper.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/01/18 14:17:32 $ $Author: felixehm $
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
      							castor::repack::RepackRequest *rreq) throw()
{
	char path[CA_MAXPATHLEN+1];
	char *server = (char*)m_ns.c_str();
	unsigned long i = 0;
	unsigned long vecsize = 0;

	std::vector<std::string>* filenamelist = new std::vector<std::string>();
	
	if ( (vecsize = getFileListSegs(rreq)) > 0 )
	{
		stage_trace(2,"Fetching filenames for %d Segments",vecsize);
		for ( i=0; i<vecsize; i++)
		{
			RepackSegment* tmp = rreq->segment().at(i);
			if (Cns_getpath (server, tmp->parent_fileid(), path) < 0) {
					stage_trace (3, "%s\n", sstrerror(serrno));
					castor::exception::Internal ex;
	    			ex.getMessage() << "FileListHelper::getFileList(..):" 
	    							<< sstrerror(serrno) << std::endl;
	    		    throw ex;
			}
			filenamelist->push_back(path);
			stage_trace(3,"%s",path);
		}
	}
	return filenamelist;
}


//------------------------------------------------------------------------------
// getFileListSegs
//------------------------------------------------------------------------------
int FileListHelper::getFileListSegs(castor::repack::RepackRequest *rreq)
{
	int flags;
	u_signed64 segs_size = 0;
	char path[CA_MAXPATHLEN+1];
	char *server = (char*)m_ns.c_str();
	char *vid = (char*)rreq->vid().c_str();
	

	struct Cns_direntape *dtp;
	Cns_list list;
	signed64 parent_fileid = -1;
	
	flags = CNS_LIST_BEGIN;
	/* all Segments from a tape belong to one Request ! */
	while ((dtp = Cns_listtape (server, vid, flags, &list)) != NULL) {
		RepackSegment* rseg= new RepackSegment();
		rseg->setVid(rreq);
		rseg->setFileid(dtp->fileid);
		rseg->setParent_fileid(dtp->parent_fileid);
		rreq->addSegment(rseg);
		stage_trace(3,"Added Segment %d %d, fileseq : %d",dtp->parent_fileid, dtp->fileid, dtp->fsec, dtp->fseq);
		segs_size += dtp->segsize;
		flags = CNS_LIST_CONTINUE;
	}
	Cns_listtape (server, vid, CNS_LIST_END, &list);
	stage_trace(2,"Size on disk to be allocated: %d", (segs_size/100000000));
	return rreq->segment().size();
}
      							

      							
	} // End namespace repack
}	//End namespace castor
