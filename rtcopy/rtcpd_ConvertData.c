/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_ConvertData.c,v $ $Revision: 1.1 $ $Date: 1999/12/02 17:22:02 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_ConvertData.c - RTCOPY server data conversion routines
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <serrno.h>

int rtcpd_VarToFix(char *inbuf, 
                   char *outbuf, 
                   int length,
                   int blocksize,
                   int recordlength,
                   int *nb_truncated,
                   void **context) {
    struct convert_statics {
        int newline_at_endbuf;
        int overflow_flag;
        char *inbuf_p;
        char *outbuf_p;
        char *outbuf_start_p;
    } *current;
    int newline;
    int nb_bytes;
    register char *p;

    if ( *context == NULL ) {
        current = (struct convert_statics *)
            calloc(1,sizeof(struct convert_statics));
        if ( current == NULL ) {
            serrno = errno;
            return(-1);
        }
        *context = (void *)current;
    } else current = (struct convert_statics *)(*context);

    if ( current->newline_at_endbuf != 0 ) {
        current->newline_at_endbuf = 0;
        return(0);
    }

    if ( current->inbuf_p == NULL ) 
        current->inbuf_p = inbuf;

    if ( current->outbuf_p == NULL ) {
        current->outbuf_p = outbuf;
        current->outbuf_start_p = outbuf;
    }

    for (;;) {
        newline = 0;

        for ( p = current->inbuf_p; p < inbuf + length; p++ ) {
            if ( *p == '\n' ) {
                newline = 1;
                break;
            }
        }

        if ( current->overflow_flag != 0 ) {
            if ( newline != 0 ) {  /* Tail part of a truncated record */
                current->overflow_flag = 0;
                (*nb_truncated)++;
            }
            current->inbuf_p = p;
            if ( current->inbuf_p >= inbuf + length ) {
                current->inbuf_p = inbuf;
                if ( newline == 0 ) return(0);
                else current->newline_at_endbuf = 1;
            } else current->inbuf_p++; /* skip newline */
        } else break;
    }  /* for (;;) */
    nb_bytes = p - current->inbuf_p;
    if ( current->outbuf_p - current->outbuf_start_p + nb_bytes > recordlength ) {
        nb_bytes = recordlength - (current->outbuf_p - current->outbuf_start_p );
        current->overflow_flag = 1;
        newline = 0;
    }
    memcpy(current->outbuf_p,current->inbuf_p,nb_bytes);
    current->inbuf_p += nb_bytes;
    current->outbuf_p += nb_bytes;

    if ( current->inbuf_p >= inbuf + length ) {
        current->inbuf_p = inbuf;
        if ( newline == 0 ) return(0);
        else current->newline_at_endbuf = 1;
    } else current->inbuf_p++;   /* skip newline */

    nb_bytes = recordlength - (current->outbuf_p - current->outbuf_start_p);
    if ( nb_bytes > 0 ) memset(current->outbuf_p,' ',nb_bytes);
    current->outbuf_p += nb_bytes;
    current->outbuf_start_p += recordlength;
    
    nb_bytes = current->outbuf_start_p - outbuf;

    if ( nb_bytes >= blocksize ) {
        current->outbuf_p = outbuf;
        current->outbuf_start_p = outbuf;
    }
    return(nb_bytes);
}

int rtcpd_FixToVar(char *inbuf,
                   char *outbuf,
                   int length,
                   int recordlength) {
    char *inbuf_p, *outbuf_p;
    int nb_bytes;
    register char *p;
    int remlen;

    inbuf_p = inbuf;
    outbuf_p = outbuf;
    remlen = length;

    while ( remlen > 0 ) {
        p = inbuf_p + ((remlen > recordlength) ? recordlength : remlen) - 1;
        while ( (p >= inbuf_p) && *p == ' ' ) p--;
        nb_bytes = p - inbuf_p +1;
        if ( nb_bytes > 0 ) memcpy(outbuf,inbuf,nb_bytes);
        outbuf_p += nb_bytes;
        *(outbuf_p++) = '\n';
        remlen -= recordlength;
        inbuf_p += recordlength;
    }
    return(outbuf_p - outbuf);
}
