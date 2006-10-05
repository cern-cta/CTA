/******************************************************************************
 *                      RepackFileChecker.cpp
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
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/



#include "RepackFileChecker.hpp"

namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileChecker::RepackFileChecker(RepackServer* svr){
  m_dbhelper = new DatabaseHelper();
  ptr_server = svr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileChecker::~RepackFileChecker(){
	delete m_dbhelper;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackFileChecker::run(void* param) throw(){
	
  RepackSubRequest* sreq = NULL;
  Cuuid_t cuuid = nullCuuid;
  FileListHelper m_filehelper (ptr_server->getNsName());

  try {
    /** get a candidate */
    sreq = m_dbhelper->checkSubRequestStatus(SUBREQUEST_READYFORSTAGING);

  }catch (castor::exception::Exception e){
      castor::dlf::Param params[] = 
      {castor::dlf::Param("Error", e.getMessage().str() )};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 1, params);
      return;
  }
  if ( sreq != NULL ){
    //m_dbhelper->lock(sreq);
    cuuid = stringtoCuuid(sreq->cuuid());
    castor::dlf::Param params[] =
    {castor::dlf::Param("VID", sreq->vid()),
      castor::dlf::Param("ID", sreq->id()),
      castor::dlf::Param("STATUS", sreq->status())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 29, 3, params);
    
    /** get the Segs for this tape  */
    if ( m_filehelper.getFileListSegs(sreq) )
      return;

    try {
    
      /** check, if we got something back */
      if ( sreq->segment().size() == 0 ){
        castor::dlf::Param params[] =
        {castor::dlf::Param("VID", sreq->vid())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 39, 1, params);
        sreq->setStatus(SUBREQUEST_DONE);
        m_dbhelper->updateSubRequest(sreq,false, cuuid);
        return;
      }
  
      /** check the filelist for multi-tapecopy repacking - we can easily
          return, because a message was written to DLF. */
      if ( checkMultiRepack(sreq) == -1 ) return; 
      
      std::vector<std::string>* filelist = m_filehelper.getFilePathnames(sreq);
      sreq->setFilesStaging(filelist->size());
      sreq->setFiles(filelist->size());
      sreq->setStatus(SUBREQUEST_TOBESTAGED);
      m_dbhelper->updateSubRequest(sreq,true, cuuid);

    }catch (castor::exception::Exception e){
      /** do nothing, messages were written by the dbhelper in case of an exception*/
    }
  }
}



//------------------------------------------------------------------------------
// checkMultiRepack
//------------------------------------------------------------------------------
int RepackFileChecker::checkMultiRepack(RepackSubRequest* sreq)
                                             throw (castor::exception::Internal)
{
  _Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  std::vector<RepackSegment*>::iterator segment = sreq->segment().begin();
  RepackSegment* retSeg = NULL;

  /** for stager request */
  struct stage_query_req request;
  struct stage_filequery_resp *responses;
  struct stage_options opts;
  struct Cns_fileid fileid;
  
  int nbresps, rc;
    
  /** iterate through the segments and check if there is another tapecopy for this segment
     in a repack process. If so, check the status and in case of STAGED or CANBEMIGR
     the file cannot be submitted as to be repacked, because the migration part would fail
     due to the not existing tapecopy (see FILERECALLED PL/SQL procedure */

  try {
    while ( segment != sreq->segment().end() ) {
      retSeg = m_dbhelper->getTapeCopy( (*segment) );

      /**  we only check, if we got an answer from the DB */
      if ( retSeg != NULL ) {
      
        /** this should never happen ! It means, that there are tapes with the 
            same TapeCopy on 2 Tapes */
        if ( retSeg->copyno() == (*segment)->copyno() ){
          castor::dlf::Param params[] =
          {castor::dlf::Param("Existing", retSeg->vid()->vid() ),
          castor::dlf::Param("To be added", sreq->vid() )};
          castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 37, 2, params);
          /// TODO: IF this error occurs, the subreques MUST be set to an invalid status
          /// so it is not taken the next time !
          return -1;
        }
        /** we create the name of the file like the stager_api likes it 
          "fileid@nameserver"  and set the query request parameter*/
        std::ostringstream buf;
        buf << (*segment)->fileid() << "@" << ptr_server->getNsName(); 
        request.type = (int)BY_FILEID;
        request.param = (void*)(buf.str().c_str());
        fileid.fileid = (*segment)->fileid();
        /// set the options
        getServiceClass(&opts, sreq);
        opts.stage_host = (char*)ptr_server->getStagerName().c_str(); 
        opts.stage_port = 0;
        opts.stage_version = 0;

        rc = errno = serrno = nbresps = 0;                                                           
        /// Send request to stager 
        rc = stage_filequery(&request,
                            1,
                            &responses,
                            &nbresps,
                            &(opts));
        if ( rc == -1 ){
          castor::dlf::Param params[] =
          {castor::dlf::Param("Error Message", sstrerror(serrno) ),
          castor::dlf::Param("Responses", nbresps )};
          castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 23, 2, params, &fileid);
          //if ( nbresps) free_filequery_resp(responses, nbresps);
          return -1;
        }
        if ( nbresps && responses[0].status != FILE_STAGEIN 
             && responses[0].status != FILE_INVALID_STATUS ) {
          castor::dlf::Param params[] =
          {castor::dlf::Param("CopyNo", retSeg->copyno() ),
          castor::dlf::Param("Existing Tape", retSeg->vid()->vid()),
          castor::dlf::Param("To be added Tape", sreq->vid())};
          castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 45, 3, params, &fileid);
          /** the file is remove from list, if invalid */
          sreq->removeSegment((*segment));
          segment--; /// removeSegment sets the pointer already to the next one.
          free_filequery_resp(responses,nbresps );
        }

        delete retSeg;
        retSeg = NULL;
      } // end if retSeg == NULL
      segment++;
    } // end while loop
  }catch (castor::exception::Exception e){
     castor::dlf::Param params[] =
     {castor::dlf::Param("ErrorMessage", e.getMessage().str() )};
     castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 23, 1, params);
     return -1;
  }
}		
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileChecker::stop() throw(){
	
}

	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

