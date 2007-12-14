
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
 * @(#)$RCSfile: FileListHelper.cpp,v $ $Revision: 1.32 $ $Release$ 
 * $Date: 2007/12/14 15:10:58 $ $Author: gtaur $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/


#include "castor/repack/FileListHelper.hpp"

namespace castor {

	namespace repack {


//------------------------------------------------------------------------------
// Constructor 
//------------------------------------------------------------------------------
FileListHelper::FileListHelper(std::string nameserver)
{
	m_ns = nameserver;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
	  FileListHelper::~FileListHelper()throw()
{

}


//------------------------------------------------------------------------------
// getFileList
//------------------------------------------------------------------------------

std::vector<u_signed64>* FileListHelper::getFileList(castor::repack::RepackSubRequest *subreq) 
  throw()
{
  double vecsize = 0;
  std::vector<RepackSegment*>::iterator iterseg;
  /** pointer to vector of all Segments */
  std::vector<u_signed64>* parentlist = new std::vector<u_signed64>();
  vecsize = subreq->repacksegment().size();
  
  /** get the cuuid from RepackSubRequest for DLF logging */
  Cuuid_t cuuid;
  cuuid = stringtoCuuid(subreq->cuuid());

  if ( vecsize > 0 ) {
    /** make up a new list with all parent_fileids */
    iterseg=subreq->repacksegment().begin();
    while ( iterseg!=subreq->repacksegment().end() ) {
      parentlist->push_back( (*iterseg)->fileid() );
      iterseg++;
    }
  }
  
  return parentlist;
}





//------------------------------------------------------------------------------
// getFilePathnames
//------------------------------------------------------------------------------
std::vector<std::string>* FileListHelper::getFilePathnames(castor::repack::RepackSubRequest *subreq)
                                              throw (castor::exception::Exception)
{
  unsigned int i=0;
  char path[CA_MAXPATHLEN+1];
  
  std::vector<u_signed64>* tmp;
  std::vector<std::string>* pathlist = new std::vector<std::string>();
  
  /** this function already checks if subreq is not NULL
     and get the parentfileids */
  tmp = getFileList(subreq);
  
  for ( i=0; i< tmp->size(); i++ )
  {
	  /** get the full path and push it into the list */
    if ( Cns_getpath((char*)m_ns.c_str(), tmp->at(i), path) < 0 ) {
      Cuuid_t cuuid;
      cuuid = stringtoCuuid(subreq->cuuid());
      castor::exception::Internal ex;
      ex.getMessage() << "FileListHelper::getFilePathnames(...):" 
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

//-----------------------------------------------------------------------------
// countFilesOnTape
//-----------------------------------------------------------------------------

u_signed64 FileListHelper::countFilesOnTape(std::string vid)
                                                 throw (castor::exception::Exception)
{
  int flags;
  struct Cns_direntape *dtp = NULL;
  Cns_list list;
  list.fd = list.eol = list.offset = list.len = 0;
  list.buf = NULL;
  serrno = SENOERR; 	// Begin:no error
  u_signed64 numSegmentLeft=0;

  if ( !vid.empty() )
  {
    /** the tape check was before ! */
    
    flags = CNS_LIST_BEGIN;
    
    while ((dtp = Cns_listtape ((char*)m_ns.c_str(),(char*) vid.c_str(), flags, &list)) != NULL) {  
      if (dtp->s_status == 'D') continue;
      numSegmentLeft++;
      flags = CNS_LIST_CONTINUE;
    }
    
    Cns_listtape ((char*)m_ns.c_str(), (char*) vid.c_str(), CNS_LIST_END, &list);               
  }
  else{
      castor::exception::Internal ex;
      ex.getMessage() << "FileListHelper::getFileListSegs(...):" 
                      <<" no vid given "  << std::endl;
      throw ex;
  }
  return  numSegmentLeft;

}
  


//------------------------------------------------------------------------------
// getFileListSegs
//------------------------------------------------------------------------------
void FileListHelper::getFileListSegs(castor::repack::RepackSubRequest *subreq)
                                                 throw (castor::exception::Exception)
{
  int flags;
  u_signed64 segs_size = 0;
  struct Cns_direntape *dtp = NULL;
  Cns_list list;
  list.fd = list.eol = list.offset = list.len = 0;
  list.buf = NULL;
  serrno = SENOERR; 	// Begin:no error
  /** get the cuuid from RepackSubRequest for DLF logging */
  Cuuid_t cuuid;
  cuuid = stringtoCuuid(subreq->cuuid());

  if ( subreq != NULL )
  {
    /** the tape check was before ! */
    
    flags = CNS_LIST_BEGIN;
    /** all Segments from a tape belong to one Request ! */
    
    while ((dtp = Cns_listtape ((char*)m_ns.c_str(), (char*)subreq->vid().c_str(), flags, &list)) != NULL) {
      
      if (dtp->s_status == 'D') continue;
      
      RepackSegment* rseg= new RepackSegment();
      rseg->setRepacksubrequest(subreq);
      rseg->setFileid(dtp->fileid);
      rseg->setFilesec(dtp->fsec);
      rseg->setCompression(dtp->compression);
      rseg->setSegsize(dtp->segsize);
      rseg->setCopyno(dtp->copyno);
      rseg->setFileseq(dtp->fseq);
      /** the Blockid -> u_signed64 */
      u_signed64 total_blockid = 0;
      total_blockid = (unsigned char)  dtp->blockid[3] * (unsigned int)0x1000000;
      total_blockid = total_blockid | (dtp->blockid[2] * 0x10000);
      total_blockid = total_blockid | (dtp->blockid[1] * 0x100);
      total_blockid = total_blockid | (dtp->blockid[0] * 0x1);
      rseg->setBlockid(total_blockid);
      subreq->addRepacksegment(rseg);
      
      segs_size += dtp->segsize;
      flags = CNS_LIST_CONTINUE;
    }
    
      Cns_listtape ((char*)m_ns.c_str(), (char*)subreq->vid().c_str(), CNS_LIST_END, &list);
      subreq->setXsize(segs_size);
         
      castor::dlf::Param params[] =
      {castor::dlf::Param("Vid", subreq->vid()),
       castor::dlf::Param("Segments", subreq->repacksegment().size()),
       castor::dlf::Param("DiskSpace", subreq->xsize())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 24, 3, params);
      
  }
  else{
      castor::exception::Internal ex;
      ex.getMessage() << "FileListHelper::getFileListSegs(...):" 
                      <<" the subrequest is null "  << std::endl;
      throw ex;
  }

}

//------------------------------------------------------------------------------
// printFileInfo
//------------------------------------------------------------------------------

void FileListHelper::printFileInfo(u_signed64 fileid, int copyno)
  throw() 
   {
    Cns_fileid file_uniqueid;
    Cns_segattrs * segattrs=NULL;
    int nbseg=0;
    int ret=0;
    int i=0;
    u_signed64 total_blockid = 0;

    memset(&file_uniqueid,'\0',sizeof(file_uniqueid));
    sprintf(file_uniqueid.server,"%s",(char*)m_ns.c_str());  
    file_uniqueid.fileid= fileid;

    ret=Cns_getsegattrs(NULL, &file_uniqueid,&nbseg,&segattrs);
    //if(file_uniqueid.server) free(file_uniqueid.server);
  
    if (ret<0){
      std::cout << "Error in retrieving file " << file_uniqueid.fileid << " from the nameserver." << std::endl;
    }
    if (nbseg == 0){
      std::cout << "File " << file_uniqueid.fileid << " not found in the nameserver." << std::endl;
    }
    ret=-1;
    for(i=0; i<nbseg; i++) {
      if (copyno == segattrs[i].copyno){ 
	ret=0;
	total_blockid = 0;
        total_blockid = (unsigned char)  segattrs[i].blockid[3] * (unsigned int)0x1000000;
	total_blockid = total_blockid | (segattrs[i].blockid[2] * 0x10000);
	total_blockid = total_blockid | (segattrs[i].blockid[1] * 0x100);
	total_blockid = total_blockid | (segattrs[i].blockid[0] * 0x1);
        std::cout <<std::setw(15)<<std::left<< file_uniqueid.fileid <<
          std::setw(10)<<std::left<< segattrs[i].copyno <<
          std::setw(10)<<std::left<< segattrs[i].fsec << 
	  std::setw(18)<<std::left<< segattrs[i].segsize <<
          std::setw(11)<<std::left<<segattrs[i].compression <<        
	  //    std::setw(10)<<std::left<<segattrs[i].s_status <<
	  std::setw(10)<<std::left<< segattrs[i].vid <<
          //    std::setw(10)<<std::left<<segattrs[i].side <<
          std::setw(10)<<std::left<<segattrs[i].fseq<<
	  std::setw(17)<<std::left<<total_blockid <<  
	  //    std::setw(12)<<std::left<<segattrs[i].checksum_name<<
	  std::setw(10)<<std::left<<std::hex<<segattrs[i].checksum<<std::dec<< std::endl;
     
      }
    }
    if (ret<0){
      std::cout << "File " << file_uniqueid.fileid << " not found in the nameserver with copyno "<< copyno << std::endl;
    }
}	  




	} // End namespace repack
}	//End namespace castor

