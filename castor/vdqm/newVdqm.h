/******************************************************************************
 *                      newVdqm.h
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
 * @(#)RCSfile: newVdqm.h  Revision: 1.0  Release Date: Jul 25, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _NEWVDQM_H_
#define _NEWVDQM_H_

#include "Castor_limits.h"

/*
 * Length of marshalled structure. Don't forget to update
 * if entries are added
 */
#define NEWVDQM_DRVREQLEN(X) ( 10 * LONGSIZE + QUADSIZE + strlen(X->volid) + \
    strlen(X->server) + strlen(X->drive) + strlen(X->dgn) + \
    strlen(X->errorHistory) + 5 )
    
/*
 * Length of marshalled structure. Don't forget to update
 * if entries are added
 */
#define NEWVDQM_VOLREQLEN(X) ( 8*LONGSIZE + \
    strlen(X->volid) + strlen(X->client_host) + strlen(X->server) + \
    strlen(X->drive) + strlen(X->dgn) + strlen(X->client_name) + 6 )    
    

typedef struct newVdqmHdr {
    int magic;
    int reqtype;
    int len;
} newVdqmHdr_t;


typedef struct newVdqmVolReq {
    int VolReqID;
    int DrvReqID;    /* Drive request ID for running requests */
    int priority;
    int client_port;
    int clientUID;
    int clientGID;
    int mode;       /* WRITE_ENABLE/WRITE_DISABLE from Ctape_constants.h */
    int recvtime;    
    char client_host[CA_MAXHOSTNAMELEN+1];
    char volid[CA_MAXVIDLEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    char drive[CA_MAXUNMLEN+1];
    char dgn[CA_MAXDGNLEN+1];
    char client_name[CA_MAXUSRNAMELEN+1];
} newVdqmVolReq_t;


typedef struct newVdqmDrvReq {
	int status;
	int DrvReqID;
	int VolReqID;          /* Volume request ID for running requests */
	int jobID;
	int recvtime;
	int resettime;         /* Last time counters were reset */
	int usecount;          /* Usage counter (total number of VolReqs so far)*/
	int errcount;          /* Drive error counter */
	int MBtransf;          /* MBytes transfered in last request. */
	int mode;       /* WRITE_ENABLE/WRITE_DISABLE from Ctape_constants.h */
	u_signed64 TotalMB;    /* Total MBytes transfered.           */
  char reqhost[CA_MAXHOSTNAMELEN+1];
	char volid[CA_MAXVIDLEN+1];
	char server[CA_MAXHOSTNAMELEN+1];
	char drive[CA_MAXUNMLEN+1];
	char dgn[CA_MAXDGNLEN+1];
	
	// Please note, that the value of errorHistory will be send through 
	// the dedicate string of the old protocol
	char errorHistory[CA_MAXLINELEN+1]; /* To send informations of the last errors to the client */
	char tapeDriveModel[CA_MAXLINELEN+1]; /* the model of the tape drive */ 
} newVdqmDrvReq_t;

#endif //_NEWVDQM_H_
