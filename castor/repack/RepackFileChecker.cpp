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
  m_dbhelper = NULL;
  ptr_server = svr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileChecker::~RepackFileChecker() throw(){
	delete m_dbhelper;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackFileChecker::run(void* param) throw(){
  if (m_dbhelper == NULL){
    m_dbhelper = new DatabaseHelper();
  }
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

    try {

      /** get the Segs for this tape  */
      m_filehelper.getFileListSegs(sreq);
      /** check, if we got something back */
      if ( sreq->segment().size() == 0 ){
        castor::dlf::Param params[] =
        {castor::dlf::Param("VID", sreq->vid())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 39, 1, params);
        sreq->setStatus(SUBREQUEST_DONE);
        m_dbhelper->updateSubRequest(sreq,false, cuuid);
        
        //stage_trace(3,"Nothing found to do for Tape %s",(char*)sreq->vid().c_str());
        return;
      }
  
      /** check the filelist for multi-tapecopy repacking - we can easily
          return, because a message was written to DLF. */
      if ( checkMultiRepack(sreq) == -1 ) return; 
      
      sreq->setFiles(sreq->segment().size());
      sreq->setStatus(SUBREQUEST_TOBESTAGED);
      m_dbhelper->updateSubRequest(sreq,true, cuuid);
      stage_trace(3,"Found %d files, RepackSubRequest for Tape %s ready for Staging ",sreq->files(),(char*)sreq->vid().c_str());
    }catch (castor::exception::Exception e){
       castor::dlf::Param params[] =
        {castor::dlf::Param("Message","Exception caugt inRepack FileChecker"), 
        castor::dlf::Param("Error Message",e.getMessage().str())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 5, 2, params);
    }
  }
}



//------------------------------------------------------------------------------
// checkMultiRepack
//------------------------------------------------------------------------------
int RepackFileChecker::checkMultiRepack(RepackSubRequest* sreq)
                                             throw (castor::exception::Exception)
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

  memset(&fileid, '\0', sizeof(Cns_fileid));
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
        getStageOpts(&opts, sreq);

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
        else {
          /** check, if the file can be send again (no diskcopy or in STAGEIN) */
          if ( nbresps
                && responses[0].status != FILE_STAGEIN  /// it is not staging in  
                && responses[0].status != FILE_INVALID_STATUS ) /// and not in invalid status
          {
            /** Give a message that the existing file has to be removed first */
            castor::dlf::Param params[] =
            {castor::dlf::Param("CopyNo", retSeg->copyno() ),
            castor::dlf::Param("Existing Tape", retSeg->vid()->vid()),
            castor::dlf::Param("To be added Tape", sreq->vid())};
            castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 45, 3, params, &fileid);
            /** the file is remove from list, if invalid */
            sreq->removeSegment((*segment));
            segment--; /// removeSegment sets the pointer already to the next one.
          }
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
  return 0;
}		
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileChecker::stop() throw(){
	
}

	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

