
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
 * @(#)$RCSfile: NsTapeGatewayHelper.cpp,v $ $Revision: 1.2 $ $Release$ 
 * $Date: 2009/01/27 09:51:44 $ $Author: gtaur $
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


int castor::tape::tapegateway::NsTapeGatewayHelper::updateMigratedFile( tape::tapegateway::FileMigratedResponse* file) throw (castor::exception::Exception){

  // set the error buffer (in rtcpclientd done with Cglobals)

  char *nsErrMsg = NULL;
  int rc = Cns_seterrbuf(nsErrMsg,512); // mysterious hardcoded 512
  if (rc <0 || nsErrMsg == NULL) return -1;
  else  *nsErrMsg = '\0';


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
  if ( nsSegAttrs == NULL )  return -1;
  

  nsSegAttrs->copyno = file->nsFileInformation()->tapeFileNsAttribute()->copyNo();
  nsSegAttrs->fsec = 1; // always one
  u_signed64 nsSize=file->nsFileInformation()->fileSize();
  nsSegAttrs->segsize = nsSize;
  int compressionFactor=0;
  if (nsSize !=0 ){
    compressionFactor= file->nsFileInformation()->fileSize()* 100 / nsSize;
  }
  /* TODO devtype check */
  
  /*
  if ( filereq->bytes_out > 0 ) {
    compressionFactor = (filereq->host_bytes * 100) / filereq->bytes_out;
    if ( (compressionFactor < 95) && (strcmp(tapereq->devtype,"3592") == 0)  )
      compressionFactor = 100;
  } else if ( (filereq->bytes_out == 0) && (strcmp(tapereq->devtype,"3592") == 0) ) {
    compressionFactor = 100;
  } else {
    compressionFactor = 0;
    } */

  strncpy(nsSegAttrs->vid, file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str(),CA_MAXVIDLEN);
  nsSegAttrs->side = file-> nsFileInformation()->tapeFileNsAttribute()->side();
  
  // pain to convert the blockid
  
  unsigned int repackBlockid=file->nsFileInformation()->tapeFileNsAttribute()->blockId(); 
  nsSegAttrs->blockid[3]=(unsigned char)((repackBlockid/0x1000000)%0x100);
  nsSegAttrs->blockid[2]=(unsigned char)((repackBlockid/0x10000)%0x100);
  nsSegAttrs->blockid[1]=(unsigned char)((repackBlockid/0x100)%0x100); 
  nsSegAttrs->blockid[0]=(unsigned char)((repackBlockid/0x1)%0x100);

  nsSegAttrs->fseq = file->nsFileInformation()->tapeFileNsAttribute()->fseq();

  strncpy(
	  nsSegAttrs->checksum_name,
	  file->nsFileInformation()->tapeFileNsAttribute()->checksumName().c_str(),
	  CA_MAXCKSUMNAMELEN
	  );
  nsSegAttrs->checksum = file->nsFileInformation()->tapeFileNsAttribute()->checksum();
  int save_serrno=0;
  time_t lastModificationTime = file->nsFileInformation()->lastModificationTime(); //... TODO
  while(1){
    serrno=0;
    rc = Cns_setsegattrs(
                          (char *)NULL, // CASTOR file name 
                          &castorFileId,
                          nbSegms,
                          nsSegAttrs,
                          lastModificationTime
                          );
      
    if (rc ==0 ) { 
      //update fine
       if ( nsSegAttrs != NULL ) free(nsSegAttrs);
       return 0;
    }
    save_serrno = serrno; 
    if ( save_serrno == ENOENT ) {
      // check if the file is still there
      char *castorFileName = '\0';
      struct Cns_filestat statbuf;
      rc = Cns_statx(castorFileName,&castorFileId,&statbuf);
      if (rc==0) {
	//the file is there
	if ( nsSegAttrs != NULL ) free(nsSegAttrs);
	return -1;
      } else {
	// we loop again to protect from 'false' ENOENT
	if ( nsSegAttrs != NULL ) free(nsSegAttrs);
	sleep(1);
	continue;
      }    
    }
    // all the rest we fail 
    if ( nsSegAttrs != NULL ) free(nsSegAttrs);
    return -1;

  }
}


int castor::tape::tapegateway::NsTapeGatewayHelper::updateRepackedFile( tape::tapegateway::FileMigratedResponse* file , std::string repackVid) throw (castor::exception::Exception){

  // set the error buffer (in rtcpclientd done with Cglobals)

  char *nsErrMsg = NULL;
  int rc = Cns_seterrbuf(nsErrMsg,512); // mysterious hardcoded 512
  if (rc <0 || nsErrMsg == NULL) return -1;
  else  *nsErrMsg = '\0';


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
  if ( nsSegAttrs == NULL )  return -1;
  

  nsSegAttrs->copyno = file->nsFileInformation()->tapeFileNsAttribute()->copyNo();
  nsSegAttrs->fsec = 1; // always one
  u_signed64 nsSize=file->nsFileInformation()->fileSize();
  nsSegAttrs->segsize = nsSize;
  int compressionFactor=0;
  if (nsSize !=0 ){
    compressionFactor= file->nsFileInformation()->fileSize() * 100 / nsSize;
  }
  /* TODO dev check */
  
  /*
  if ( filereq->bytes_out > 0 ) {
    compressionFactor = (filereq->host_bytes * 100) / filereq->bytes_out;
    if ( (compressionFactor < 95) && (strcmp(tapereq->devtype,"3592") == 0)  )
      compressionFactor = 100;
  } else if ( (filereq->bytes_out == 0) && (strcmp(tapereq->devtype,"3592") == 0) ) {
    compressionFactor = 100;
  } else {
    compressionFactor = 0;
    } */

  strncpy(nsSegAttrs->vid,file->nsFileInformation()->tapeFileNsAttribute()->vid().c_str(),CA_MAXVIDLEN);
  nsSegAttrs->side = file->nsFileInformation()->tapeFileNsAttribute()->side();
  
  // pain to convert the blockid
  
  unsigned int repackBlockid=file->nsFileInformation()->tapeFileNsAttribute()->blockId(); 
  nsSegAttrs->blockid[3]=(unsigned char)((repackBlockid/0x1000000)%0x100);
  nsSegAttrs->blockid[2]=(unsigned char)((repackBlockid/0x10000)%0x100);
  nsSegAttrs->blockid[1]=(unsigned char)((repackBlockid/0x100)%0x100); 
  nsSegAttrs->blockid[0]=(unsigned char)((repackBlockid/0x1)%0x100);

  nsSegAttrs->fseq = file->nsFileInformation()->tapeFileNsAttribute()->fseq();

  strncpy(
            nsSegAttrs->checksum_name,
            file->nsFileInformation()->tapeFileNsAttribute()->checksumName().c_str(),
            CA_MAXCKSUMNAMELEN
            );
  nsSegAttrs->checksum = file->nsFileInformation()->tapeFileNsAttribute()->checksum();

  serrno = rc = 0; 

  
  // we are in repack case so the copy should be replaced
  
  int oldNbSegms=0;
  struct Cns_segattrs *oldSegattrs=NULL;

  // get the old copy with the segments associated

  rc = Cns_getsegattrs( NULL, (struct Cns_fileid *)&castorFileId, &oldNbSegms, &oldSegattrs);

  if ( (rc == -1) || (oldNbSegms <= 0) || (oldSegattrs == NULL) ) 
    return(-1);

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
 
  if (nbSegments == 0) return -1; //file not found

  nbSegments=0; // now I count the number of segments

  for(int i = 0; i < oldNbSegms; i++) {
    if ( nsSegAttrs->copyno = oldSegattrs[i].copyno ) nbSegments++;
  }
       
  // just in case of a sigle segment I check the checksum
	 
  if (  nbSegments  == 1 && oldSegattrs->checksum != nsSegAttrs->checksum)  return -1; // INVALID checksum
  
  // replace the tapecopy
  
  time_t lastModificationTime = file->nsFileInformation()->lastModificationTime();

  rc = Cns_replacetapecopy(&castorFileId, repackVid.c_str(),nsSegAttrs->vid,nbSegms,nsSegAttrs,lastModificationTime);
       
  if (oldSegattrs) free(oldSegattrs);
  if (nsSegAttrs) free (nsSegAttrs);
  oldSegattrs = NULL;
  nsSegAttrs = NULL;
  return rc;

}

int  castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToMigrate(castor::tape::tapegateway::FileToMigrateResponse* file) throw (castor::exception::Exception) {
  
  if ( file == NULL || file->nsFileInformation() == NULL || file->nsFileInformation()->tapeFileNsAttribute()->vid().empty()) {
    return -1;
  }

  // set the error buffer (in rtcpclientd done with Cglobals)

  char *nsErrMsg = NULL;
  int rc = Cns_seterrbuf(nsErrMsg,512); // mysterious hardcoded 512
  if (rc <0 || nsErrMsg == NULL) return -1;
  else  *nsErrMsg = '\0';

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
  
 
  rc = Cns_getsegattrs(NULL,&castorFileId,&nbSegs,&segArray);
  if ( rc == -1 ) return -1;

  int nbCopies=0;
  // let's check if there is already a file on that tape ... completely changed check dual copies

  for ( int i=0; i< nbSegs ;i++) {
    if ( strcmp(segArray[i].vid ,(file->nsFileInformation()->tapeFileNsAttribute()->vid()).c_str()) == 0 )
      return -1; // copy already on that tape
    nbCopies++;
  }
  
  // check the fileclass to verify the number of tapecopies allowed

  struct Cns_filestat statbuf;
  struct Cns_fileclass fileClass;
  char castorFileName[CA_MAXPATHLEN+1];

  *castorFileName = '\0';
  rc = Cns_statx(castorFileName,&castorFileId,&statbuf);
  if ( rc == -1 ) return(-1);
    
     
  memset(&fileClass,'\0',sizeof(fileClass));
  rc = Cns_queryclass( castorFileId.server,statbuf.fileclass, NULL,&fileClass );
  if ( rc == -1 ) return -1 ;
  if ( fileClass.tppools != NULL ) free(fileClass.tppools);

    
  if ( nbCopies >= fileClass.nbcopies ) return -1; // extra check to avoid exceeded number of tapecopies

  return fileClass.nbcopies; // return the number of tapecopies allowed   

}


int castor::tape::tapegateway::NsTapeGatewayHelper::checkFileToRecall(castor::tape::tapegateway::FileToRecallResponse* file) throw (castor::exception::Exception) {
  
  if ( file == NULL || file->nsFileInformation() == NULL ||  file->nsFileInformation()->tapeFileNsAttribute() == NULL ) {
    return -1;
  }

  // set the error buffer (in rtcpclientd done with Cglobals)

  char *nsErrMsg = NULL;
  int rc = Cns_seterrbuf(nsErrMsg,512); // mysterious hardcoded 512
  if (rc <0 || nsErrMsg == NULL) return -1;
  else  *nsErrMsg = '\0';

 
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
  
 
  rc = Cns_getsegattrs(NULL,&castorFileId,&nbSegs,&segArray);
  if ( rc == -1 ) return -1;


  // let's get the copy number that we are refering to

  for ( int i=0; i< nbSegs ;i++) {
    if ( segArray[i].copyno == file->nsFileInformation()->tapeFileNsAttribute()->copyNo()) {
      // here it is the right segment we want to recall
      file->nsFileInformation()->tapeFileNsAttribute()->setFsec(segArray[i].fsec);
      file->nsFileInformation()->tapeFileNsAttribute()->setBlockId(segArray[i].blockid[0]+segArray[i].blockid[1]*0x100+ segArray[i].blockid[2]*0x10000 + segArray[i].blockid[3]*0x1000000);
      file->nsFileInformation()->tapeFileNsAttribute()->setCompression(segArray[i].compression);
      file->nsFileInformation()->tapeFileNsAttribute()->setSide(segArray[i].side);
      file->nsFileInformation()->tapeFileNsAttribute()->setChecksumName(segArray[i].checksum_name);
      file->nsFileInformation()->tapeFileNsAttribute()->setChecksum(segArray[i].checksum);
      file->nsFileInformation()->tapeFileNsAttribute()->setVid(segArray[i].vid);
      if (file->nsFileInformation()->tapeFileNsAttribute()->blockId()==0) file->setPositionCommandCode(TPPOSIT_FSEQ);
      return 0;
    }
  }
  return -1;
}

int castor::tape::tapegateway::NsTapeGatewayHelper::getFileOwner(castor::tape::tapegateway::NsFileInformation* nsInfo, u_signed64 &uid, u_signed64 &gid ) throw (castor::exception::Exception){

  if ( nsInfo  == NULL ) {
    return -1;
  }

  // set the error buffer (in rtcpclientd done with Cglobals)

  char *nsErrMsg = NULL;
  int rc = Cns_seterrbuf(nsErrMsg,512); // mysterious hardcoded 512
  if (rc <0 || nsErrMsg == NULL) return -1;
  else  *nsErrMsg = '\0';

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
  rc = Cns_statx(castorFileName,&castorFileId,&statbuf);
  if ( rc == -1 ) return -1;
  
  uid = (int)statbuf.uid;
  gid = (int)statbuf.gid;

  return 0;

}

int  castor::tape::tapegateway::NsTapeGatewayHelper::checkRecalledFile(castor::tape::tapegateway::FileRecalledResponse* file) throw (castor::exception::Exception) {

   if ( file == NULL || file->nsFileInformation() == NULL || file->nsFileInformation() == NULL ||
       file->nsFileInformation()->tapeFileNsAttribute() == NULL || file->nsFileInformation()->tapeFileNsAttribute()->vid().empty()) {
    return -1;
  }

  // set the error buffer (in rtcpclientd done with Cglobals)

  char *nsErrMsg = NULL;
  int rc = Cns_seterrbuf(nsErrMsg,512); // mysterious hardcoded 512
  if (rc <0 || nsErrMsg == NULL) return -1;
  else  *nsErrMsg = '\0';

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

  rc = Cns_getsegattrs( NULL, &castorFileId,&nbSegs, &segattrs);
  
  if ( (rc == -1) || (nbSegs <= 0) || (segattrs == NULL) ) return -1;

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
      unsigned int repackBlockid= file->nsFileInformation()->tapeFileNsAttribute()->blockId(); 
      char convertedBlockId[4];
      convertedBlockId[3]=(unsigned char)((repackBlockid/0x1000000)%0x100);
      convertedBlockId[2]=(unsigned char)((repackBlockid/0x10000)%0x100);
      convertedBlockId[1]=(unsigned char)((repackBlockid/0x100)%0x100); 
      convertedBlockId[0]=(unsigned char)((repackBlockid/0x1)%0x100);
      if ( memcmp(segattrs[i].blockid,convertedBlockId, sizeof(convertedBlockId)) != 0)  continue;
    }

    if ( segattrs[i].fsec != file->nsFileInformation()->tapeFileNsAttribute()->fsec() ) continue;
    if ( segattrs[i].copyno != file->nsFileInformation()->tapeFileNsAttribute()->copyNo() ) continue;
    
    // All the checks were successfull let's check checksum  

    // check the checksum (mandatory)

    if (file->nsFileInformation()->tapeFileNsAttribute()->checksumName().empty() && segattrs[i].checksum_name[0] == '\0') return 1; // no checksum

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

      int rc = Cns_updateseg_checksum(
                                  castorFileId.server,
                                  castorFileId.fileid,
                                  &segattrs[i],
                                  newSegattrs
                                  );
      free(newSegattrs);
      if (rc<0) return -1;
      return 1;

    }

    if (strcmp(file->nsFileInformation()->tapeFileNsAttribute()->checksumName().c_str(),segattrs[i].checksum_name)== 0 && file->nsFileInformation()->tapeFileNsAttribute()->checksum() != segattrs[i].checksum ) return 0; // checksum is correct

    else return -1; // you cannot change checksum on the fly

  }

  return -1; // segment not found at all

}
