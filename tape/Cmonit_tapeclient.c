/*
 * $id$
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/* 
 * Cmonit_tapeclient.c - Sends monitoring info to the Monitoring Daemon
 */

#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

#include "marshall.h"
#include "Cmonit_api.h"
#include "serrno.h"
#include "Cnetdb.h"
#include "Ctape.h"


/*
 * This method sends a packet to the Monitoring daemon with the state of all the drives in the server.
 */
int DLL_DECL Cmonit_send_tape_status(tape_table, nbtpdrives)
  struct tptab *tape_table;
  int nbtpdrives; 
{
  char buf[CA_MAXUDPPLEN];
  struct tptab *tunp;
   char *sbp;
   char *nbpos, *start_content;
   int send_data = 1;
   int cur_tp_drive = 0;
   int nb_sent, sd;

   /* Checking the initialization */
   if (Cmonit_init() == -1) {
     return(-1);
   }
   
   /* Creating the socket used for subsequent sends */
   sd = socket(AF_INET, SOCK_DGRAM, 0); 
   if (sd == -1) {
     serrno = EMON_SYSTEM;
    return(-1);
   }

  sbp = buf;
  tunp = tape_table;

  while (send_data) {

    marshall_LONG(sbp, MREC_MAGIC);
    marshall_LONG(sbp, MREC_TAPE);
    nbpos = sbp;
    marshall_LONG(sbp, 0);
    start_content = sbp;
 /*     marshall_WORD (sbp, nbtpdrives); */
    
    while ( (send_data= (cur_tp_drive<nbtpdrives))
	    && ( (sbp - buf ) < ( CA_MAXUDPPLEN - sizeof(struct tptab) ) )) {

      marshall_LONG (sbp, tunp->uid);
      marshall_LONG (sbp, tunp->gid);
      marshall_LONG (sbp, tunp->jid);
      marshall_STRING (sbp, tunp->dgn);
      marshall_WORD (sbp, tunp->up);
      marshall_WORD (sbp, tunp->asn);
      marshall_LONG (sbp, tunp->asn_time);
      marshall_STRING (sbp, tunp->drive);
      marshall_WORD (sbp, tunp->mode);
/*        marshall_STRING (sbp, labels[tunp->lblcode]); */
      marshall_WORD (sbp, tunp->tobemounted);
      marshall_STRING (sbp, tunp->vid);
      marshall_STRING (sbp, tunp->vsn);
      if (tunp->filp) {
	marshall_LONG (sbp, tunp->filp->cfseq);
      } else {
	marshall_LONG (sbp, 0);
      }
      tunp++;
      cur_tp_drive++;
    } /* End While send_data= */

    marshall_LONG(nbpos, sbp - start_content );

    /* Sending the packet */
    nb_sent = Cmonit_send(sd, buf, (sbp - buf));
    if (nb_sent <= 0) {
      close(sd);
      return(-1);
    }
  } /* End While(send_data) */
  close(sd);
  return(0);
}






