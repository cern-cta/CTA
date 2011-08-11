/******************************************************************************
 *                      NsTapeGatewayHelper.cpp
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
 * @(#)$RCSfile: NsTapeGatewayHelper.cpp,v $ $Revision: 1.15 $ $Release$ 
 * $Date: 2009/08/10 22:07:12 $ $Author: murrayc3 $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <common.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "Cns_api.h"
#include "rfio_api.h"

#include "castor/exception/OutOfMemory.hpp"

#include "castor/tape/tapegateway/PositionCommandCode.hpp"

#include "castor/tape/tapegateway/daemon/NsTapeGatewayHelper.hpp"


void castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile( tape::tapegateway::FileMigratedNotification& file, int copyNumber, std::string vid, u_signed64 lastModificationTime) throw (castor::exception::Exception){ 

  // fill in the castor file id structure
  int nbSegs=0;
  struct Cns_segattrs *nsSegAttrs = NULL;


  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file.nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file.fileid();


  // let's get the number of copies allowed

  char castorFileName[CA_MAXPATHLEN+1];
  struct Cns_filestat statbuf;
  memset(&statbuf,'\0',sizeof(statbuf));
  *castorFileName = '\0';
  
  serrno=0;
  int rc = Cns_statx(castorFileName,&castorFileId,&statbuf);
  if ( rc == -1 ) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkMigratedFile:"
      << "impossible to stat the file";
    throw ex;
  }

  struct Cns_fileclass fileClass;
  memset(&fileClass,'\0',sizeof(fileClass));
  
  serrno=0;
  rc = Cns_queryclass( castorFileId.server,
		       statbuf.fileclass,
		       NULL,
		       &fileClass
		       );
      
  if ( rc == -1 ) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile:"
      << "impossible to get file class";
    throw ex;
  }
      
  if ( fileClass.nbcopies != 1 ){
    // get segments for this fileid
    
    serrno=0;
    int rc = Cns_getsegattrs(NULL,&castorFileId,&nbSegs,&nsSegAttrs);
    if ( rc == -1 ) {
      if (nsSegAttrs != NULL ) free(nsSegAttrs);
      nsSegAttrs=NULL;
      castor::exception::Exception ex(serrno);
      ex.getMessage()
	<< "castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile:"
	<< "impossible to get the file";
      throw ex;
    }

    // check done just if there is the chance of multiple copies
    
    for ( int i=0; i< nbSegs ;i++) {
      if ( strcmp(nsSegAttrs[i].vid , vid.c_str()) == 0 ) { 
	if (nsSegAttrs != NULL ) free(nsSegAttrs);
	nsSegAttrs=NULL;
	castor::exception::Exception ex(EEXIST);
	ex.getMessage()
	  << "castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile:"
	  << "this file have already a copy on that tape";
	throw ex;
      }
    }
    
    if (nsSegAttrs != NULL ) free(nsSegAttrs);
    nsSegAttrs=NULL;

  }
  


  // fill in the information of the tape file to update the nameserver

  nbSegs = 1; // NEVER more than one segment

  nsSegAttrs = (struct Cns_segattrs *)calloc(
                                             nbSegs,
                                             sizeof(struct Cns_segattrs)
                                             );
  if ( nsSegAttrs == NULL ) {
    castor::exception::Exception ex(-1);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile:"
      << "calloc failed";
    throw ex;

  } 
  

  nsSegAttrs->copyno = copyNumber; 
  nsSegAttrs->fsec = 1; // always one
  u_signed64 nsSize=file.fileSize();
  nsSegAttrs->segsize = nsSize;
  u_signed64 compression = (file.fileSize() * 100 )/ file.compressedFileSize();
  nsSegAttrs->compression = compression;
  strncpy(nsSegAttrs->vid, vid.c_str(),CA_MAXVIDLEN);
  nsSegAttrs->side = 0; //HARDCODED
  nsSegAttrs->s_status = '-';
  
  //  convert the blockid
  
  nsSegAttrs->blockid[3]=file.blockId3(); 
  nsSegAttrs->blockid[2]=file.blockId2();
  nsSegAttrs->blockid[1]=file.blockId1();
  nsSegAttrs->blockid[0]=file.blockId0();

  nsSegAttrs->fseq = file.fseq();

  strncpy(
	  nsSegAttrs->checksum_name,
	  file.checksumName().c_str(),
	  CA_MAXCKSUMNAMELEN
	  );
  nsSegAttrs->checksum = file.checksum();
  int save_serrno=0;

  // removed the logic of "retry 5 times to update"
  serrno=0;
  rc = Cns_setsegattrs(
		       (char *)NULL, // CASTOR file name 
		       &castorFileId,
		       nbSegs,
		       nsSegAttrs,
		       lastModificationTime
		       );
      
  if (rc ==0 ) { 
    //update fine
    if ( nsSegAttrs != NULL ) free(nsSegAttrs);
    return;
  }

  save_serrno = serrno; 
  if ( save_serrno == ENOENT ) {
    // check if the file is still there
    char *castorFileName = '\0';
    struct Cns_filestat statbuf;
    rc = Cns_statx(castorFileName,&castorFileId,&statbuf);
    if (rc==0) {
      //the file is there the update failed
      if ( nsSegAttrs != NULL ) free(nsSegAttrs);
      castor::exception::Exception ex(EINVAL);
      ex.getMessage()
	<< "castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile:"
	<< "false enoent received the update failed";
      throw ex;
    } else {
      // the file is not there the update is considered successfull 
      if ( nsSegAttrs != NULL ) free(nsSegAttrs);
      return;
    }    
  }
  // all the other possible serrno cause the failure
  if ( nsSegAttrs != NULL ) free(nsSegAttrs);
  castor::exception::Exception ex(save_serrno);
  ex.getMessage()
    << "castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile:"
    << "Cns_setsegattrs failed";
  throw ex;
  
}


void castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile(
    tape::tapegateway::FileMigratedNotification& file, std::string repackVid,
    int copyNumber, std::string vid, u_signed64 lastModificationTime)
throw (castor::exception::Exception){
  // fill in the castor file id structure
  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file.nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file.fileid();

  // fill in the information of the tape file to update the nameserver
  struct Cns_segattrs nsSegAttrs;
  memset (&nsSegAttrs, '\0', sizeof (struct Cns_segattrs));

  nsSegAttrs.copyno = copyNumber;
  nsSegAttrs.fsec = 1; // always one
  u_signed64 compression = (file.fileSize() * 100 )/ file.compressedFileSize();
  nsSegAttrs.compression =compression;
  nsSegAttrs.segsize = file.fileSize();
  nsSegAttrs.s_status = '-';
  
  strncpy(nsSegAttrs.vid,vid.c_str(),CA_MAXVIDLEN);
  nsSegAttrs.vid[CA_MAXVIDLEN]='\0';
  nsSegAttrs.side = 0; // HARDCODED side
  //  convert the blockid
  nsSegAttrs.blockid[3]=file.blockId3();
  nsSegAttrs.blockid[2]=file.blockId2();
  nsSegAttrs.blockid[1]=file.blockId1();
  nsSegAttrs.blockid[0]=file.blockId0();
  nsSegAttrs.fseq = file.fseq();
  strncpy(
            nsSegAttrs.checksum_name,
            file.checksumName().c_str(),
            CA_MAXCKSUMNAMELEN
            );
  nsSegAttrs.checksum = file.checksum();
 
  // we are in repack case so the copy should be replaced
  int oldNbSegms=0;
  struct Cns_segattrs *oldSegattrs=NULL;
  // get the old copy with the segments associated
  serrno=0;
  int rc = Cns_getsegattrs( NULL, (struct Cns_fileid *)&castorFileId, &oldNbSegms, &oldSegattrs);
  if ( (rc == -1) || (oldNbSegms <= 0) || (oldSegattrs == NULL) )  {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << "impossible to get the old segment";
    if (oldSegattrs) free (oldSegattrs);
    throw ex;
  }

  // get the file class
  char castorFileName[CA_MAXPATHLEN+1]; *castorFileName = '\0';
  struct Cns_filestatcs nsFileAttrs;
  memset(&nsFileAttrs,'\0',sizeof(nsFileAttrs));
  serrno=0;
  rc = Cns_statcsx(castorFileName,&castorFileId,&nsFileAttrs);
  if ( rc == -1 ) {
    NoSuchFileException ex;
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkRepackedFile:"
      << "impossible to stat the file: rc=-1, serrno=" << serrno;
    throw ex;
  }

  struct Cns_fileclass nsFileClass;
  memset(&nsFileClass,'\0',sizeof(nsFileClass));
  serrno=0;
  rc = Cns_queryclass( castorFileId.server,
      nsFileAttrs.fileclass,
      NULL,
      &nsFileClass
  );
  if ( rc == -1 ) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << "impossible to get file class";
    throw ex;
  }
      
  if ( nsFileClass.nbcopies != 1 ){
    // check if there is already a copy of this file in the new tape
    for ( int i=0; i< oldNbSegms ;i++) {
      if ( strcmp(oldSegattrs[i].vid , vid.c_str()) == 0 ) { 
	if (oldSegattrs) free(oldSegattrs);
	castor::exception::Exception ex(EEXIST);
	ex.getMessage()
	  << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
	  << "this file have already a copy on that tape";
	throw ex;
      }
    }
  }

  bool copyToOverwriteFound=false;
  int  copyToOverwriteNumber=-1;
  // We have all the segments and copies of that file
  // first round we get the right copy number using the vid since the stager
  // picks a random copy number
  for(int i = 0; i < oldNbSegms; i++) {
    // at this point we know the vid so we can get the copy number to replace
    // the stager gave a random one
    if(!strcmp(oldSegattrs[i].vid, repackVid.c_str())) {
      // it is tape copy
      // XXX In the absence or a rigorous tracking of the fseq and other parameters,
      // we take the VID match as good enough
      copyToOverwriteNumber = nsSegAttrs.copyno = oldSegattrs[i].copyno;
      copyToOverwriteFound=true;
      break;
    }
  }
 
  if (!copyToOverwriteFound) {
    // The copy we expected to replace can't be found. We can't proceed further
    // with repack in any case. This situation can be a harmless update/removal
    // of the file during repack, or an internal problem.
    // Let's confirm the positive case and finish with this outdated request.
    // As we don't have any other criteria, we compare the file's size and then checksum with this
    // segment's checksum. If there is no match, this tapecopy does not match the file anymore:
    // => warn and leave.
    // If the checksums matches, log an error (but leave anyway as the initial goal, repacking
    // the repackVID tape is reached.
    // Offcial comparison method for checksum is in Cns_srv_setsegattrs, and it
    // is a sprintf/strcmp combo. (segments store checksums as numbers, while file
    // stores is as string).

    // Confirm the file has changed in the mean time:
    bool changeConfirmed = false;
    if (nsFileAttrs.filesize != file.fileSize()) {
      changeConfirmed = true;
    } else {
      if (((strcmp(nsSegAttrs.checksum_name, "adler32") == 0) &&
          (strcmp(nsFileAttrs.csumtype, "AD") == 0))
      ) {
        char chkSumStr[CA_MAXCKSUMLEN+1];
        snprintf(chkSumStr, CA_MAXCKSUMLEN+1, "%lx", nsSegAttrs.checksum);
        if (!strncmp(chkSumStr, nsFileAttrs.csumvalue, CA_MAXCKSUMLEN))
          changeConfirmed = true;
      }
    }
    // We return the conclusion to the caller in the form of an exception
    if (oldSegattrs) free(oldSegattrs);
    if (changeConfirmed) {
      FileMutatedException ex;
      ex.getMessage()
          << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
          << " segment not found: file has changed.";
      throw ex;
    } else {
      FileMutationUnconfirmedException ex;
      ex.getMessage()
          << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
          << " segment not found: file change not confirmed.";
      throw ex;
    }
  }

  int nbSegments=0; // now I count the number of segments
  for(int i = 0; i < oldNbSegms; i++) {
    if ( nsSegAttrs.copyno == oldSegattrs[i].copyno ) nbSegments++;
  }
  // just in case of a single segment I check the checksum
  if (  nbSegments  == 1 && oldSegattrs->checksum != nsSegAttrs.checksum) {
    if (oldSegattrs) free(oldSegattrs);
    oldSegattrs = NULL;
    castor::exception::Exception ex(SECHECKSUM);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << " invalid checksum";
    throw ex; 
  }  

  // replace the tapecopy: only one at a time 
  rc = Cns_replacetapecopy(&castorFileId, repackVid.c_str(),nsSegAttrs.vid,1,&nsSegAttrs,lastModificationTime);
  if (oldSegattrs) free(oldSegattrs);
  oldSegattrs = NULL;
  if (rc<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << " impossible to replace tapecopy for copynb=" << copyToOverwriteNumber << " rc=" << rc;
    throw ex; 
  }
}

void castor::tape::tapegateway::NsTapeGatewayHelper::getBlockIdToRecall(castor::tape::tapegateway::FileToRecall& file, std::string vid ) throw (castor::exception::Exception) {

 
  // get segments for this fileid 
 

  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file.nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file.fileid();

  int nbSegs=0;
  struct Cns_segattrs *segArray = NULL;
  serrno=0;
  int rc = Cns_getsegattrs(NULL,&castorFileId,&nbSegs,&segArray);

  if ( rc == -1 ) {

    if (segArray != NULL )   free(segArray);
    segArray=NULL;
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::getBlockIdToRecall:"
      << "Cns_getsegattrs failed";
    throw ex;
  }

  // let's get the copy number that we are refering to

  for ( int i=0; i< nbSegs ;i++) {
    if ( segArray[i].fseq == file.fseq() &&  strcmp(segArray[i].vid , vid.c_str()) == 0 ) {
      // here it is the right segment we want to recall
      // impossible to have 2 copies of the same file in the same aggregate   

      file.setBlockId0(segArray[i].blockid[0]);
      file.setBlockId1(segArray[i].blockid[1]);
      file.setBlockId2(segArray[i].blockid[2]);
      file.setBlockId3(segArray[i].blockid[3]);
 
      //if ( memcmp(blockid,nullblkid,sizeof(blockid)) != 0 ) 
      
      if ((file.blockId0() == '\0') && (file.blockId1() == '\0') && ( file.blockId2()== '\0' ) && ( file.blockId3() == '\0' )){
	file.setPositionCommandCode(TPPOSIT_FSEQ); // magic things for the first file
      }
	
      if (segArray != NULL )   free(segArray);
      segArray=NULL;
      return;
    }
  }
  
  // segment not found anymore

  if (segArray != NULL )   free(segArray);
  segArray=NULL;
  castor::exception::Exception ex(ENOENT);
  ex.getMessage()
    << "castor::tape::tapegateway::NsTapeGatewayHelper::getBlockIdToRecall:"
    << "impossible to find the segment";
  throw ex;
}


void  castor::tape::tapegateway::NsTapeGatewayHelper::checkRecalledFile(castor::tape::tapegateway::FileRecalledNotification& file, std::string vid, int copyNb) throw (castor::exception::Exception) {
 
 
  // get segments for this fileid

  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file.nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file.fileid();


  int nbSegs=0;
  struct Cns_segattrs *segattrs = NULL;
  
  serrno=0;
  int rc = Cns_getsegattrs( NULL, &castorFileId,&nbSegs, &segattrs);
  
  if ( (rc == -1) || (nbSegs <= 0) || (segattrs == NULL) ) {
    if (segattrs != NULL ) free(segattrs);
    segattrs=NULL;
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkRecalledFile:"
      << "Cns_getsegattrs failed";
    throw ex;

  }

  // Now we have the information from the nameserver and we can check the information given as input

  // Find the segment given (for old multisegmented file this function will be called several times) 

  // Let's get the right segment


  for ( int i=0; i<nbSegs; i++ ) {

    if ( strcmp(segattrs[i].vid, vid.c_str()) != 0 ) continue;
  
    if ( segattrs[i].side != 0 ) continue;

    if ( segattrs[i].fseq != file.fseq() ) continue;

    // removed the  check of  the blockid 
   
    if ( segattrs[i].copyno != copyNb ) continue;

    // All the checks were successfull let's check checksum  

    // check the checksum (mandatory)

    if (file.checksumName().empty() && segattrs[i].checksum_name[0] == '\0') {
      return; //no checksum given
    }

    if (segattrs[i].checksum_name[0] == '\0') {
      // old segment without checksum we set the new one

      struct Cns_segattrs *newSegattrs;
      memcpy(&newSegattrs,&segattrs[i],sizeof(segattrs));
      strncpy(
              newSegattrs->checksum_name,
              file.checksumName().c_str(),
              CA_MAXCKSUMNAMELEN
              );
      newSegattrs->checksum = file.checksum(); 
      serrno=0;
      int rc = Cns_updateseg_checksum(
                                  castorFileId.server,
                                  castorFileId.fileid,
                                  &segattrs[i],
                                  newSegattrs
                                  );
      if (segattrs != NULL ) free(segattrs);
      segattrs=NULL;
      if (newSegattrs)free(newSegattrs);
      newSegattrs=NULL;
      
      if (rc<0) {
	castor::exception::Exception ex(serrno);
	ex.getMessage()
	  << "castor::tape::tapegateway::NsTapeGatewayHelper::checkRecalledFile:"
	  << "Cns_updateseg_checksum failed";
	throw ex;
      }

      return;
    }

    if ( strcmp(file.checksumName().c_str(),segattrs[i].checksum_name) != 0 || file.checksum() != segattrs[i].checksum ) {
      // wrong checksum and you cannot change the checksum on the fly
      
      if (segattrs != NULL ) free(segattrs);
      segattrs=NULL;

      castor::exception::Exception ex(SECHECKSUM);
      ex.getMessage()
	<< "castor::tape::tapegateway::NsTapeGatewayHelper::checkRecalledFile:"
	<< "wrong checksum";
      throw ex;
    } else {

      if (segattrs != NULL ) free(segattrs);
      segattrs=NULL;

      // correct checksum
      return;
    }
  }
  // segment not found at all

  
  if (segattrs != NULL ) free(segattrs);
  segattrs=NULL;
  castor::exception::Exception ex(ENOENT);
  ex.getMessage()
    << "castor::tape::tapegateway::NsTapeGatewayHelper::checkRecalledFile:"
    << "segment not found";
  throw ex;

}


void  castor::tape::tapegateway::NsTapeGatewayHelper::checkFileSize(castor::tape::tapegateway::FileRecalledNotification& file) throw (castor::exception::Exception) {
 
  // get segments for this fileid

  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file.nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file.fileid();

  struct Cns_filestat statBuf;

  char path[CA_MAXPATHLEN+1];
  int rc = Cns_getpath(castorFileId.server, castorFileId.fileid,(char*)path);
  if ( rc == -1) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileSize:"
      << "Cns_getpath failed for nshost "<<castorFileId.server<<" fileid "<<castorFileId.fileid;
    throw ex;
  }
  
  serrno=0; 
  
  // in rtcpclientd it is using the castorfile.filesize
  rc = Cns_statx( path, &castorFileId,&statBuf);
  
  if ( rc == -1) {
   
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileSize:"
      << "Cns_statx failed for "<<path;
    throw ex;

  }

  // get with rfio the filesize of the copy on disk

  
  struct stat64 st;
  serrno=0;

  rc = rfio_stat64((char*)file.path().c_str(),&st);
 
  if (rc <0 ){
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileSize:"
      << "rfio_stat failed for "<<file.path().c_str();
    throw ex;

  }
 
  if ( (u_signed64)st.st_size !=  statBuf.filesize){ // cast done to follow castor convention 
    castor::exception::Exception ex(ERTWRONGSIZE);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileSize:"
      << "wrong file size, ns filesize is "<< st.st_size <<"and on the diskcopy has size "<<statBuf.filesize ;
    throw ex;  

  }
  
}

// This checker function will raise an exception if the Fseq forseen for writing
// is not strictly greater than the highest know Fseq for this tape in the name
// server (meaning an overwrite).
void castor::tape::tapegateway::NsTapeGatewayHelper::checkFseqForWrite (const std::string &vid, int side, int Fseq)
     throw (castor::exception::Exception) {
  struct Cns_segattrs segattrs;
  memset (&segattrs, 0, sizeof(struct Cns_segattrs));
  int rc = Cns_lastfseq (vid.c_str(), side, &segattrs);
  // Read serrno only once as it points at a function hidden behind a macro.
  int save_serrno = serrno;
  // If the name server does not know about the tape, we're safe (return, done).
  if ((-1 == rc) && (ENOENT == save_serrno)) return;
  if (rc != 0) { // Failure to contact the name server and all other errors.
    castor::exception::Exception ex(save_serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFseqForWrite:"
      << "Cns_lastfseq failed for VID="<<vid.c_str()<< " side="<< side << " : rc="
      << rc << " serrno =" << save_serrno << "(" << sstrerror(save_serrno) << ")";
    throw ex;
  }
  if (Fseq <= segattrs.fseq) { // Major error, we are about to overwrite an fseq
    // referenced in the name server.
    castor::exception::Exception ex(ERTWRONGFSEQ);
    ex.getMessage()
          << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFseqForWrite:"
          << "Fseq check failed for VID="<<vid.c_str()<< " side="<< side << " Fseq="
          << Fseq << " last referenced segment in NS=" << segattrs.fseq;
    throw ex;
  }
}

