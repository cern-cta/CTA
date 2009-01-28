
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
 * @(#)$RCSfile: NsTapeGatewayHelper.cpp,v $ $Revision: 1.3 $ $Release$ 
 * $Date: 2009/01/28 15:42:30 $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#include "castor/tape/tapegateway/NsTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/TapeFileNsAttribute.hpp"
#include "castor/tape/tapegateway/PositionCommandCode.hpp"
#include "osdep.h"
#include "Cns_api.h"
#include <common.h>
#include <time.h>
#include <sys/time.h>



void castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile( tape::tapegateway::FileMigratedResponse* file) throw (castor::exception::Exception){
  // check input

  if (file == NULL || file->nsFileInformation() == NULL || file->nsFileInformation()->tapeFileNsAttribute() ) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile:"
      << "invalid input";
    throw ex;
  }

  // fill in the castor file id structure
  
  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file-> nsFileInformation()->nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file->nsFileInformation()->fileid();


  // fill in the information of the tape file to update the nameserver

  int nbSegms = 1; // NEVER more than one segment

  struct Cns_segattrs * nsSegAttrs = (struct Cns_segattrs *)calloc(
                                             nbSegms,
                                             sizeof(struct Cns_segattrs)
                                             );
  if ( nsSegAttrs == NULL ) {
    castor::exception::Exception ex(-1);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile:"
      << "calloc failed";
    throw ex;

  } 
  

  nsSegAttrs->copyno = file->nsFileInformation()->tapeFileNsAttribute()->copyNo();
  nsSegAttrs->fsec = 1; // always one
  u_signed64 nsSize=file->nsFileInformation()->fileSize();
  nsSegAttrs->segsize = nsSize;
  nsSegAttrs->compression =file->nsFileInformation()->tapeFileNsAttribute()->compression();
  strncpy(nsSegAttrs->vid, file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str(),CA_MAXVIDLEN);
  nsSegAttrs->side = file-> nsFileInformation()->tapeFileNsAttribute()->side();
  
  //  convert the blockid
  
  unsigned int blockidValue=file->nsFileInformation()->tapeFileNsAttribute()->blockId(); 
  nsSegAttrs->blockid[3]=(unsigned char)((blockidValue/0x1000000)%0x100);
  nsSegAttrs->blockid[2]=(unsigned char)((blockidValue/0x10000)%0x100);
  nsSegAttrs->blockid[1]=(unsigned char)((blockidValue/0x100)%0x100); 
  nsSegAttrs->blockid[0]=(unsigned char)((blockidValue/0x1)%0x100);

  nsSegAttrs->fseq = file->nsFileInformation()->tapeFileNsAttribute()->fseq();

  strncpy(
	  nsSegAttrs->checksum_name,
	  file->nsFileInformation()->tapeFileNsAttribute()->checksumName().c_str(),
	  CA_MAXCKSUMNAMELEN
	  );
  nsSegAttrs->checksum = file->nsFileInformation()->tapeFileNsAttribute()->checksum();
  int save_serrno=0;
  time_t lastModificationTime = file->nsFileInformation()->lastModificationTime();

  // removed the logic of "retry 5 times to update"
  serrno=0;
  int rc = Cns_setsegattrs(
		       (char *)NULL, // CASTOR file name 
		       &castorFileId,
		       nbSegms,
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


void castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile( tape::tapegateway::FileMigratedResponse* file , std::string repackVid) throw (castor::exception::Exception){

  // check input

  if (file == NULL || file->nsFileInformation() == NULL || file->nsFileInformation()->tapeFileNsAttribute() ) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << "invalid input";
    throw ex;
  }

  // fill in the castor file id structure
  
  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file->nsFileInformation()->nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file->nsFileInformation()->fileid();


  // fill in the information of the tape file to update the nameserver

  int nbSegms = 1; // NEVER more than one segment

  struct Cns_segattrs * nsSegAttrs = (struct Cns_segattrs *)calloc(
                                            nbSegms,
                                             sizeof(struct Cns_segattrs)
                                             );
  if ( nsSegAttrs == NULL ) {
    castor::exception::Exception ex(-1);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << "calloc failed";
    throw ex;

  } 

  nsSegAttrs->copyno = file->nsFileInformation()->tapeFileNsAttribute()->copyNo();
  nsSegAttrs->fsec = 1; // always one
  nsSegAttrs->compression =file->nsFileInformation()->tapeFileNsAttribute()->compression();
  nsSegAttrs->segsize = file->nsFileInformation()->fileSize();
  
  strncpy(nsSegAttrs->vid,file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str(),CA_MAXVIDLEN);
  nsSegAttrs->side = file->nsFileInformation()->tapeFileNsAttribute()->side();
  
  //  convert the blockid
  
  unsigned int blockidValue=file->nsFileInformation()->tapeFileNsAttribute()->blockId(); 
  nsSegAttrs->blockid[3]=(unsigned char)((blockidValue/0x1000000)%0x100);
  nsSegAttrs->blockid[2]=(unsigned char)((blockidValue/0x10000)%0x100);
  nsSegAttrs->blockid[1]=(unsigned char)((blockidValue/0x100)%0x100); 
  nsSegAttrs->blockid[0]=(unsigned char)((blockidValue/0x1)%0x100);

  nsSegAttrs->fseq = file->nsFileInformation()->tapeFileNsAttribute()->fseq();

  strncpy(
            nsSegAttrs->checksum_name,
            file->nsFileInformation()->tapeFileNsAttribute()->checksumName().c_str(),
            CA_MAXCKSUMNAMELEN
            );
  nsSegAttrs->checksum = file->nsFileInformation()->tapeFileNsAttribute()->checksum();
 
  // we are in repack case so the copy should be replaced
  
  int oldNbSegms=0;
  struct Cns_segattrs *oldSegattrs=NULL;

  // get the old copy with the segments associated

  serrno=0;
  int rc = Cns_getsegattrs( NULL, (struct Cns_fileid *)&castorFileId, &oldNbSegms, &oldSegattrs);

  if ( (rc == -1) || (oldNbSegms <= 0) || (oldSegattrs == NULL) )  {
   
    if (nsSegAttrs) free (nsSegAttrs);
    nsSegAttrs = NULL;

    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << "impossible to get the old segment";
    throw ex;

  } 

  int nbSegments=0;

  // we have all the segments and copies of that file
  
  // first round we get the right copy number using the vid since the stager pick a random one

  for(int i = 0; i < oldNbSegms; i++) {
    // at this point we know the vid so we can get the copy number to replace
    // the stager gave a random one	
    if(!strcmp(oldSegattrs[i].vid, repackVid.c_str())) {
      // it is our tape copy 
      // I update just the first time then I just continue to loop to see if it is a multisegment file
      nsSegAttrs->copyno = oldSegattrs[i].copyno;
      nbSegments=1;
      break;
    }
  }
 
  if (nbSegments == 0) {
    if (oldSegattrs) free(oldSegattrs);
    if (nsSegAttrs) free (nsSegAttrs);
    oldSegattrs = NULL;
    nsSegAttrs = NULL;

    castor::exception::Exception ex(ENOENT);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << "segment not found";
    throw ex; 

  }

  nbSegments=0; // now I count the number of segments

  for(int i = 0; i < oldNbSegms; i++) {
    if ( nsSegAttrs->copyno = oldSegattrs[i].copyno ) nbSegments++;
  }
       
  // just in case of a sigle segment I check the checksum
	 
  if (  nbSegments  == 1 && oldSegattrs->checksum != nsSegAttrs->checksum) {

    if (oldSegattrs) free(oldSegattrs);
    if (nsSegAttrs) free (nsSegAttrs);
    oldSegattrs = NULL;
    nsSegAttrs = NULL;
    castor::exception::Exception ex(SECHECKSUM);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << "invalid checksum";
    throw ex; 

  }  

  // replace the tapecopy
  
  time_t lastModificationTime = file->nsFileInformation()->lastModificationTime();

  rc = Cns_replacetapecopy(&castorFileId, repackVid.c_str(),nsSegAttrs->vid,nbSegms,nsSegAttrs,lastModificationTime);
       
  if (oldSegattrs) free(oldSegattrs);
  if (nsSegAttrs) free (nsSegAttrs);
  oldSegattrs = NULL;
  nsSegAttrs = NULL;
  
  if (rc<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile:"
      << "impossible to replace tapecopy";
    throw ex; 

  }

}

void  castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToMigrate(castor::tape::tapegateway::FileToMigrateResponse* file) throw (castor::exception::Exception) {
  
  
  // check input

  if (file == NULL || file->nsFileInformation() == NULL || file->nsFileInformation()->tapeFileNsAttribute() ) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToMigrate:"
      << "invalid input";
    throw ex;
  }


  // get segments for this fileid

  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file->nsFileInformation()->nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file->nsFileInformation()->fileid();


  int nbSegs=0;
  struct Cns_segattrs *segArray = NULL;
  
  serrno=0;
  int rc = Cns_getsegattrs(NULL,&castorFileId,&nbSegs,&segArray);
  if ( rc == -1 ) {
    if (segArray != NULL ) free(segArray);
    segArray=NULL;

    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToMigrate:"
      << "impossible to get the file";
    throw ex;
  }

  int nbCopies=0; // to count the number of copies of this file in the nameserver

  // let's check if there is already a file on that tape 

  for ( int i=0; i< nbSegs ;i++) {
    if ( strcmp(segArray[i].vid ,(file->nsFileInformation()->tapeFileNsAttribute()->vid()).c_str()) == 0 ) {
      if (segArray != NULL ) free(segArray);
      segArray=NULL;
      castor::exception::Exception ex(EEXIST);
      ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToMigrate:"
      << "this file have already a copy on that tape";
      throw ex;
    }
    nbCopies++;
  }
  
  // check the fileclass to verify the number of tapecopies allowed

  struct Cns_filestat statbuf;
  struct Cns_fileclass fileClass;
  char castorFileName[CA_MAXPATHLEN+1];

  *castorFileName = '\0';
  serrno=0;
  rc = Cns_statx(castorFileName,&castorFileId,&statbuf);
  
  if ( rc == -1 ) {
    if (segArray != NULL ) free(segArray);
    segArray=NULL;
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToMigrate:"
      << "Cns_statx failed";
    throw ex;
  } 
     
  memset(&fileClass,'\0',sizeof(fileClass));
  serrno=0;
  rc = Cns_queryclass( castorFileId.server,statbuf.fileclass, NULL,&fileClass );
  if ( rc == -1 ) {
    if (segArray != NULL ) free(segArray);
    segArray=NULL;
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToMigrate:"
      << "Cns_queryclass failed";
    throw ex;
  } 
    
  if ( nbCopies >= fileClass.nbcopies ) {// extra check to avoid exceeded number of tapecopies
      if (segArray != NULL ) free(segArray);
      segArray=NULL;
      castor::exception::Exception ex(-1); 
      ex.getMessage()
	<< "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToMigrate:"
	<< "too many tapecopies";
      throw ex;
  }
 
  if (segArray != NULL ) free(segArray);
  segArray=NULL;

}


void castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToRecall(castor::tape::tapegateway::FileToRecallResponse* file) throw (castor::exception::Exception) {

  // check input

  if (file == NULL || file->nsFileInformation() == NULL || file->nsFileInformation()->tapeFileNsAttribute() ) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToRecall:"
      << "invalid input";
    throw ex;
  }

 
  // get segments for this fileid 

  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file->nsFileInformation()->nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file->nsFileInformation()->fileid();

  int nbSegs=0;
  struct Cns_segattrs *segArray = NULL;
  serrno=0;
  int rc = Cns_getsegattrs(NULL,&castorFileId,&nbSegs,&segArray);

  if ( rc == -1 ) {
    if (segArray != NULL )   free(segArray);
    segArray=NULL;
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToRecall:"
      << "Cns_getsegattrs failed";
    throw ex;
  }

  // let's get the copy number that we are refering to

  for ( int i=0; i< nbSegs ;i++) {
    if ( segArray[i].copyno == file->nsFileInformation()->tapeFileNsAttribute()->copyNo() && segArray[i].fsec == file->nsFileInformation()->tapeFileNsAttribute()->fsec() ) {
      // here it is the right segment we want to recall
      file->nsFileInformation()->tapeFileNsAttribute()->setBlockId(segArray[i].blockid[0]+segArray[i].blockid[1]*0x100+ segArray[i].blockid[2]*0x10000 + segArray[i].blockid[3]*0x1000000);
      file->nsFileInformation()->tapeFileNsAttribute()->setCompression(segArray[i].compression);
      file->nsFileInformation()->tapeFileNsAttribute()->setSide(segArray[i].side);
      file->nsFileInformation()->tapeFileNsAttribute()->setChecksumName(segArray[i].checksum_name);
      file->nsFileInformation()->tapeFileNsAttribute()->setChecksum(segArray[i].checksum);
      file->nsFileInformation()->tapeFileNsAttribute()->setVid(segArray[i].vid);
      if (file->nsFileInformation()->tapeFileNsAttribute()->blockId()==0) file->setPositionCommandCode(TPPOSIT_FSEQ);
      if (segArray != NULL )   free(segArray);
      segArray=NULL;
      return;
    }
  }

  if (segArray != NULL )   free(segArray);
  segArray=NULL;
  castor::exception::Exception ex(ENOENT);
  ex.getMessage()
    << "castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToRecall:"
    << "impossible to find the segment";
  throw ex;
}


// function used just to log information, for the moment it is not used

void  castor::tape::tapegateway::NsTapeGatewayHelper::getFileOwner(castor::tape::tapegateway::NsFileInformation* nsInfo, u_signed64 &uid, u_signed64 &gid ) throw (castor::exception::Exception){

  if ( nsInfo  == NULL ) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
    << "castor::tape::tapegateway::NsTapeGatewayHelper::getFileOwner:"
    << "invalid input";
    throw ex;
  }

  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          nsInfo->nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = nsInfo->fileid();

  struct Cns_filestat statbuf;
  char castorFileName[CA_MAXPATHLEN+1];

  *castorFileName = '\0';
  serrno=0;
  int rc = Cns_statx(castorFileName,&castorFileId,&statbuf);
  if ( rc == -1 ) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
    << "castor::tape::tapegateway::NsTapeGatewayHelper::getFileOwner:"
    << "Cns_statx failed";
    throw ex;
  };
  
  uid = (int)statbuf.uid;
  gid = (int)statbuf.gid;

}

void  castor::tape::tapegateway::NsTapeGatewayHelper::checkRecalledFile(castor::tape::tapegateway::FileRecalledResponse* file) throw (castor::exception::Exception) {
  // check input

  if (file == NULL || file->nsFileInformation() == NULL || file->nsFileInformation()->tapeFileNsAttribute() ) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()
      << "castor::tape::tapegateway::NsTapeGatewayHelper::checkRecalledFile:"
      << "invalid input";
    throw ex;
  }

  // get segments for this fileid

  struct Cns_fileid castorFileId;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          file->nsFileInformation()->nshost().c_str(),
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = file->nsFileInformation()->fileid();


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

    if ( strcmp(segattrs[i].vid,file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str()) != 0 ) continue;
    if ( segattrs[i].side != file->nsFileInformation()->tapeFileNsAttribute()->side()) continue;
    if ( segattrs[i].fseq != file->nsFileInformation()->tapeFileNsAttribute()->fseq() ) continue;
    // check the blockid depending the positioning method
    if ( file->positionCommandCode() == TPPOSIT_BLKID) {
      // get the block id in the nameserver format
      unsigned int blockidValue= file->nsFileInformation()->tapeFileNsAttribute()->blockId(); 
      char convertedBlockId[4];
      convertedBlockId[3]=(unsigned char)((blockidValue/0x1000000)%0x100);
      convertedBlockId[2]=(unsigned char)((blockidValue/0x10000)%0x100);
      convertedBlockId[1]=(unsigned char)((blockidValue/0x100)%0x100); 
      convertedBlockId[0]=(unsigned char)((blockidValue/0x1)%0x100);
      if ( memcmp(segattrs[i].blockid,convertedBlockId, sizeof(convertedBlockId)) != 0)  continue;
    }

    if ( segattrs[i].fsec != file->nsFileInformation()->tapeFileNsAttribute()->fsec() ) continue;
    if ( segattrs[i].copyno != file->nsFileInformation()->tapeFileNsAttribute()->copyNo() ) continue;
    
    // All the checks were successfull let's check checksum  

    // check the checksum (mandatory)

    if (file->nsFileInformation()->tapeFileNsAttribute()->checksumName().empty() && segattrs[i].checksum_name[0] == '\0') {
      return; //no checksum given
    }

    if (segattrs[i].checksum_name[0] == '\0') {
      // old segment without checksum we set the new one

      struct Cns_segattrs *newSegattrs;
      memcpy(&newSegattrs,&segattrs[i],sizeof(segattrs));
      strncpy(
              newSegattrs->checksum_name,
              file->nsFileInformation()->tapeFileNsAttribute()->checksumName().c_str(),
              CA_MAXCKSUMNAMELEN
              );
      newSegattrs->checksum = file->nsFileInformation()->tapeFileNsAttribute()->checksum(); 
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

    if ( strcmp(file->nsFileInformation()->tapeFileNsAttribute()->checksumName().c_str(),segattrs[i].checksum_name) != 0 || file->nsFileInformation()->tapeFileNsAttribute()->checksum() != segattrs[i].checksum ) {
      // wrong checksum and you cannot change the checksum on the fly
      
      if (segattrs != NULL ) free(segattrs);
      segattrs=NULL;

      castor::exception::Exception ex(serrno);
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
