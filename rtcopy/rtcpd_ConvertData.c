/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd_ConvertData.c - RTCOPY server data conversion routines
 */

#include <stdio.h>
#include <stdlib.h>
#define _XOPEN_SOURCE
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <serrno.h>
#include <Cuuid.h>
#include <rtcp_constants.h>

#define SWAPIT(X) { int __a; \
    swab((char *)&(X),(char *)&__a, sizeof((X))); \
    (X) = ((unsigned int)__a <<16) | ((unsigned int)__a >>16); }



int rtcpd_VarToFix(char *inbuf, 
                   char *outbuf, 
                   int length,
                   int blocksize,
                   int recordlength,
                   int *bytes_used,
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

    /*
     * Allocate save context for static data
     */
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
    if ( bytes_used != NULL ) *bytes_used = nb_bytes + 1; /* Disk file record */
    if ( current->outbuf_p - current->outbuf_start_p + nb_bytes > recordlength ) {
        nb_bytes = recordlength - (current->outbuf_p - current->outbuf_start_p );
        current->overflow_flag = 1;
        newline = 0;
    }
    memcpy(current->outbuf_p,current->inbuf_p,nb_bytes);
    current->inbuf_p += nb_bytes;
    current->outbuf_p += nb_bytes;

    if ( current->inbuf_p >= inbuf + length ) {
        current->inbuf_p = NULL;
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
        if ( nb_bytes > 0 ) memcpy(outbuf_p,inbuf_p,nb_bytes);
        outbuf_p += nb_bytes;
        *(outbuf_p++) = '\n';
        remlen -= recordlength;
        inbuf_p += recordlength;
    }
    return(outbuf_p - outbuf);
}

/*
 * Convert from FORTRAN sequential format to fix. Really not
 * useful to anybody except L3 who asked for it. It implies
 * that the FORTRAN file must have been written in F format to
 * tape instead of U format. This routine only works for files
 * with fixed record length which must be equal to the record
 * length specified with -L. The routine removes the fortran
 * control words from each of the records in the input buffer
 * and copies the rest back to the buffer. Note that the enclosing
 * lrecl words are NOT counted to the recrod which means that the
 * actual logical records are lrecl + 8 bytes.
 */
int rtcpd_f77RecToFix(char *buffer,
                      int length,
                      int lrecl,
                      char *errmsgtxt,
                      void **context) {
    struct f77conv_statics {
        int do_swap;
        int in_p;
    } *current;
    union {
        char c[4];
        int lrecl;
    } f77rec;
    int save_in_p = 0;
    int out_len, remlen;

    /*
     * Allocate save context for static data
     */
    if ( *context == NULL ) {
        current = (struct f77conv_statics *)
            calloc(1,sizeof(struct f77conv_statics));
        if ( current == NULL ) {
            serrno = errno;
            return(-1);
        }
        current->do_swap = -1;
        current->in_p = 0;
        *context = (void *)current;
    } else current = (struct f77conv_statics *)(*context);

    save_in_p = current->in_p;
    out_len = remlen = length;
    remlen -= current->in_p;
    while ( remlen > 0 ) {
        memcpy(f77rec.c,&buffer[current->in_p],4);
        if ( current->do_swap == -1 ) {
            if ( f77rec.lrecl == lrecl ) current->do_swap = 0;
            else {
                SWAPIT(f77rec.lrecl);
                if ( f77rec.lrecl == lrecl ) current->do_swap = 1;
            }
        } else if ( current->do_swap == 1 ) SWAPIT(f77rec.lrecl);

        if ( f77rec.lrecl != lrecl ) {
            if ( errmsgtxt != NULL ) 
                sprintf(errmsgtxt,RT149,"CPTPDSK",lrecl,f77rec.lrecl);
            return(-1);
        }

        if ( current->in_p == 0 ) {
            out_len -= 4;
            memmove(&buffer[current->in_p],&buffer[current->in_p+4],remlen);
        } else if ( current->in_p >=4 ) {
            out_len -= 8;
            current->in_p -= 4;
            memmove(&buffer[current->in_p],&buffer[current->in_p+8],remlen);
        }

        current->in_p += f77rec.lrecl < remlen ? f77rec.lrecl + 4 : remlen;
        remlen = out_len - current->in_p;
    }

    /* 
     * Remove the enclosing control word for the last record
     */
    if ( remlen == 0 ) out_len -= 4; 
    
    /*
     * Calculate new offset.
     */
    current->in_p = (f77rec.lrecl+8 - (length - save_in_p) % 
        (f77rec.lrecl + 8) % (f77rec.lrecl + 8) );

    return(out_len);
}

