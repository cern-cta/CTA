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
 * @(#)$RCSfile: FileListHelper.cpp,v $ $Revision: 1.10 $ $Release$ $Date: 2006/02/17 18:56:16 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/


#include "castor/repack/FileListHelper.hpp"

namespace castor {

	namespace repack {


//------------------------------------------------------------------------------
// Constructor 1
//------------------------------------------------------------------------------
FileListHelper::FileListHelper(std::string nameserver)
{
	m_ns = (char*)nameserver.c_str();
}


//------------------------------------------------------------------------------
// Constructor 2, initialises the m_ns from the castor config file
//------------------------------------------------------------------------------
FileListHelper::FileListHelper() throw (castor::exception::Internal) {
	
	if ( !(m_ns = getconfent("CNS", "HOST",0)) ){
		castor::exception::Internal ex;
		ex.getMessage() << "Unable to initialise FileListHelper with nameserver "
		<< "entry in castor config file";
		throw ex;		
	}
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileListHelper::~FileListHelper()
{
	delete m_ns;
}

//------------------------------------------------------------------------------
// getFilePathnames
//------------------------------------------------------------------------------
std::vector<std::string>* FileListHelper::getFilePathnames(
								castor::repack::RepackSubRequest *subreq, Cuuid_t& cuuid) throw()
{
	int i=0;
	char path[CA_MAXPATHLEN+1];
	
	std::vector<u_signed64>* tmp;
	std::vector<std::string>* pathlist = new std::vector<std::string>();

	/* this function already checks if subreq is not NULL */
	/* and get the parentfileids */
	tmp = getFileList(subreq, cuuid);

	stage_trace(2,"Fetching filenames for %f Files",tmp->size());
	for ( i=0; i< tmp->size(); i++ )
	{
		/* get the full path and push it into the list */
		if ( Cns_getpath(m_ns, tmp->at(i), path) < 0 ) {
				castor::exception::Internal ex;
				ex.getMessage() << "FileListHelper::getFileList(..):" 
								<< sstrerror(serrno) << std::endl;
				castor::dlf::Param params[] =
				{castor::dlf::Param("Standard Message", sstrerror(ex.code()))};
				castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 15, 1, params);
			    throw ex;
		}
		else{
			std::cout << path << std::endl;
			pathlist->push_back(path);
		}
	}
	return pathlist;
}


//------------------------------------------------------------------------------
// getFileList, check if double entries are in list (should never happen !!!)
//------------------------------------------------------------------------------
std::vector<u_signed64>* FileListHelper::getFileList(
      							castor::repack::RepackSubRequest *subreq, Cuuid_t& cuuid) throw()
{
	unsigned long i = 0;
	double vecsize = 0;
	std::vector<RepackSegment*>::iterator iterseg;
	/* pointer to vector of all Segments */
	std::vector<u_signed64>* parentlist = new std::vector<u_signed64>();
	vecsize = subreq->segment().size();

	if ( vecsize > 0 ) {
		/* make up a new list with all parent_fileids */
		iterseg=subreq->segment().begin();
		while ( iterseg!=subreq->segment().end() ) {
			parentlist->push_back( (*iterseg)->fileid() );
			iterseg++;
		}
	}
	
	/* sort this list and remove double entries */
	sort(parentlist->begin(),parentlist->end());
	std::vector<u_signed64>::iterator j=parentlist->begin();
	u_signed64 fileid=0;
	
	while ( j!= parentlist->end() ){
		if ( fileid == (*j) ){
			std::cerr << "ERROR : Double entry for fileid " << (*j) 
					  << "on tape " << subreq->vid() << std::endl;
		}
		else{
			fileid = (*j);
		}
		j++;
	}
	stage_trace(3,"Number of parents: %d",parentlist->size());
	return parentlist;
}


//------------------------------------------------------------------------------
// getFileListSegs
//------------------------------------------------------------------------------
int FileListHelper::getFileListSegs(castor::repack::RepackSubRequest *subreq, Cuuid_t& cuuid)
{
	int flags;
	u_signed64 segs_size = 0;
	struct Cns_direntape *dtp = NULL;
	Cns_list list;
	list.fd = list.eol = list.offset = list.len = 0;
	list.buf = NULL;

	
	if ( subreq != NULL )
	{
		/* the tape check was before ! */
	
		flags = CNS_LIST_BEGIN;
		/* all Segments from a tape belong to one Request ! */
		while ((dtp = Cns_listtape (m_ns, (char*)subreq->vid().c_str(), flags, &list)) != NULL) {
			
			RepackSegment* rseg= new RepackSegment();
			rseg->setVid(subreq);
			rseg->setFileid(dtp->fileid);
			rseg->setFilesec(dtp->fsec);
			rseg->setCompression(dtp->compression);
			rseg->setSegsize(dtp->segsize);
			subreq->addSegment(rseg);

			segs_size += dtp->segsize;
			flags = CNS_LIST_CONTINUE;
			/* TODO:just for debug 
			std::cout //<< path  
					  << " fileid " << dtp->fileid
					  << " filesec " << dtp->fsec
					  << " fileseq " << dtp->fseq 
					  << " block1 " << (unsigned short)dtp->blockid[0] 
					  << " block2 " << (unsigned short)dtp->blockid[1] 
					  << " block3 " << (unsigned short)dtp->blockid[2] 
					  << " block4 " << (unsigned short)dtp->blockid[3] 
					  << std::endl;
			*/
		}
		Cns_listtape (m_ns, (char*)subreq->vid().c_str(), CNS_LIST_END, &list);
		stage_trace(2,"Size on disk to be allocated: %u", segs_size);
		subreq->setXsize(segs_size);

		castor::dlf::Param params[] =
     		 {castor::dlf::Param("Vid", subreq->vid()),
     		  castor::dlf::Param("Segments", subreq->segment().size()),
     		  castor::dlf::Param("DiskSpace", subreq->xsize())};
		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 24, 3, params);
		
		return subreq->segment().size();
	}
	return -1;
}

	} // End namespace repack
}	//End namespace castor
