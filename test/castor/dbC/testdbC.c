/******************************************************************************
 *                      testdb.cpp
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
 * @(#)$RCSfile: testdbC.c,v $ $Revision: 1.4 $ $Release$ $Date: 2004/06/01 15:38:15 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/rh/Request.h"
#include "castor/rh/FileRequest.h"
#include "castor/rh/StageInRequest.h"
#include "castor/rh/File.h"
#include "castor/rh/Client.h"
#include "castor/Services.h"
#include "castor/BaseAddress.h"
#include "castor/db/DbAddress.h"
#include "castor/IObject.h"
#include "castor/IClient.h"
#include "castor/Constants.h"
#include <stdio.h>

int main (int argc, char** argv) {
  // Prepare a request
  struct Crh_StageInRequest_t* fr;
  struct Crh_FileRequest_t* frfr;
  struct Crh_Request_t* frr;
  struct C_IObject_t* frio;
  struct Crh_Client_t *cl;
  struct C_IClient_t *clic;
  struct Crh_File_t* f1;
  struct Crh_File_t* f2;
  struct C_Services_t* svcs;
  struct C_BaseAddress_t *ad;
  struct C_IAddress_t *adia;
  struct Cdb_DbAddress_t *ad2;
  struct C_BaseAddress_t *ad2ba;
  struct C_IAddress_t *ad2ia;
  struct C_IObject_t* fr2Obj;
  struct Crh_Request_t* fr2r;
  struct Crh_FileRequest_t* fr2;
  unsigned long id;

  Crh_StageInRequest_create(&fr);
  frfr = Crh_StageInRequest_getFileRequest(fr);
  frr  = Crh_FileRequest_getRequest(frfr);
  frio = Crh_Request_getIObject(frr);
    
  Crh_Client_create(&cl);
  clic = Crh_Client_getIClient(cl);
  Crh_Client_setIpAddress(cl, 0606);
  Crh_Client_setPort(cl, 0707);
  C_IClient_setRequest(clic, frr);
  Crh_Request_setClient(frr, clic);

  Crh_File_create(&f1);
  Crh_File_setName(f1, "First test File");
  Crh_FileRequest_addFiles(frfr, f1);
  Crh_File_setRequest(f1, frfr);

  Crh_File_create(&f2);
  Crh_File_setName(f2, "2nd test File");
  Crh_FileRequest_addFiles(frfr, f2);
  Crh_File_setRequest(f2, frfr);

  // Get a Services instance
  C_Services_create(&svcs);

  // Stores the request
  C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &ad);
  adia = C_BaseAddress_getIAddress(ad);
  if (C_Services_createRep(svcs, adia, frio, 1) != 0) {
    printf("Error caught in createRep.\n");
    // release the memory
    C_BaseAddress_delete(ad);
    C_Services_delete(svcs);
    Crh_StageInRequest_delete(fr);
    return 1;
  }
  C_BaseAddress_delete(ad);

  // Retrieves it in a separate object
  Crh_StageInRequest_id(fr, &id);
  Cdb_DbAddress_create(id, "OraCnvSvc", SVC_ORACNV, &ad2);
  ad2ba = Cdb_DbAddress_getBaseAddress(ad2);
  ad2ia = C_BaseAddress_getIAddress(ad2ba);
  if (C_Services_createObj(svcs,
                           ad2ia,
                           &fr2Obj) != 0) {
    printf("Error caught in createObj\n.");
    Cdb_DbAddress_delete(ad2);
    C_Services_delete(svcs);
    C_IObject_delete(fr2Obj);
    return 1;
  }
  Cdb_DbAddress_delete(ad2);
  fr2r = Crh_Request_fromIObject(fr2Obj);
  fr2 = Crh_FileRequest_fromRequest(fr2r);
  
  // Display both objects for comparison
  printf("Originally :\n");
  Crh_StageInRequest_print(fr);
  printf("Finally :\n");
  Crh_FileRequest_print(fr2);

  C_Services_delete(svcs);
  Crh_StageInRequest_delete(fr);
  Crh_FileRequest_delete(fr2);
  return 0;
}
