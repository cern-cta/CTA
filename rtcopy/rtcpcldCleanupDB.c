/******************************************************************************
 *                      cleanupDB.c
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
 * @(#)$RCSfile: rtcpcldCleanupDB.c,v $ $Revision: 1.10 $ $Release$ $Date: 2007/02/23 09:30:11 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#include <errno.h>
#include <string.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#if !defined(_WIN32)
#include <unistd.h>
#endif /* !_WIN32 */
#include <net.h>
#include <osdep.h>
#include <log.h>
#include <Castor_limits.h>
#include <Cuuid.h>
#include <Ctape_constants.h>
#include <castor/stager/Tape.h>
#include <castor/stager/Segment.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <castor/stager/FileClass.h>
#include <castor/stager/ITapeSvc.h>
#include <castor/Services.h>
#include <castor/BaseObject.h>
#include <castor/BaseAddress.h>
#include <castor/IAddress.h>
#include <castor/IObject.h>
#include <castor/IClient.h>
#include <castor/Constants.h>
#include <serrno.h>
#include <stdio.h>
#include <stdlib.h>
#include <rtcp_constants.h>
#include <Ctape_api.h>
#include <stage.h>
#include <Cmutex.h>
#include <Cglobals.h>
#include <common.h>
#include <vdqm_api.h>
#include <Cgetopt.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld_messages.h>
#include <rtcpcld.h>
#include <rtcpcldapi.h>

void usage(char *cmd) 
{
  fprintf(stderr,"Usage: %s [-V VID -s side -m mode] [-k key]\n",
          cmd);
  return;
}

int main(int argc, char *argv[]) 
{
  int c, rc, mode = -1, side = -1;
  char *vid = NULL;
  struct Cstager_Tape_t *tp = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t **dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct Cstager_ITapeSvc_t **tpSvc = NULL;
  unsigned long key = 0;

  /* Initializing the C++ log */
  /* Necessary at start of program and after any fork */
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);

  Coptind = 1;
  Copterr = 1;
  while ( (c = Cgetopt(argc,argv,"V:s:m:k:")) != -1 ) {
    switch(c) {
    case 'V':
      vid = Coptarg;
      break;
    case 's':
      side = atoi(Coptarg);
      break;
    case 'k':
      key = atol(Coptarg);
      break;
    case 'm':
      mode = atoi(Coptarg);
      break;
    }
  }
  
  if ( (key == 0) && (vid == NULL || side < 0 || mode < 0) ) {
    usage(argv[0]);
    return(2);
  }

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 || dbSvc == NULL || *dbSvc == NULL ) {
    fprintf(stderr,"rtcpcld_getDbSvc(): %s",sstrerror(serrno));
    return(-1);
  }

  if ( key == 0 ) {
    rc = rtcpcld_getStgSvc(&tpSvc);
    if ( rc == -1 || tpSvc == NULL || *tpSvc == NULL ) {
      fprintf(stderr,"rtcpcld_getDbSvc(): %s\n",sstrerror(serrno));
      return(1);
    }
    
    rc = Cstager_ITapeSvc_selectTape(
                                       *tpSvc,
                                       &tp,
                                       vid,
                                       side,
                                       mode
                                       );
    if ( rc == -1 ) {
      fprintf(stderr,"Cstager_ITapeSvc_selectTape(): %s, DB ERROR=%s\n",
              sstrerror(serrno),
              Cstager_ITapeSvc_errorMsg(*tpSvc));
      return(1);
    }
  } else {
    Cstager_Tape_create(&tp);
    Cstager_Tape_setId(tp,key);
  }
  
  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) {
    fprintf(stderr,"C_BaseAddress_create(): %s",sstrerror(serrno));
    return(1);
  }

  C_BaseAddress_setCnvSvcName(baseAddr,"DbCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_DBCNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_Tape_getIObject(tp);

  if ( key != 0 ) {
    rc = C_Services_updateObj(*dbSvc,iAddr,iObj);
    if ( rc == -1 ) {
      fprintf(stderr,"C_Services_updateObj(key=%lu): %s, DB_ERROR=%s\n",
              (unsigned long)key,
              sstrerror(serrno),
              C_Services_errorMsg(*dbSvc));
      return(1);
    }
  }

  rc = C_Services_deleteRep(*dbSvc,iAddr,iObj,1);
  if ( rc != 0 ) {
    fprintf(stderr,"C_Services_deleteRep(): %s, DB_ERROR=%s\n",
            sstrerror(serrno),
            C_Services_errorMsg(*dbSvc));
  } else {
    (void)C_Services_rollback(*dbSvc,iAddr);
  }

  C_IAddress_delete(iAddr);
  C_Services_delete(*dbSvc);
  if ( rc != 0 ) return(1);
  else return(0);
}
