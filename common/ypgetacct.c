/*
 * $Id: ypgetacct.c,v 1.2 1999/07/22 14:47:18 obarring Exp $
 * $Log: ypgetacct.c,v $
 * Revision 1.2  1999/07/22 14:47:18  obarring
 * MT safe version
 *
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "$Id: ypgetacct.c,v 1.2 1999/07/22 14:47:18 obarring Exp $";
#endif /* not lint */

/* ypgetacct()              Getting account id in YP      */

#if defined(NIS)
#include <stdio.h>
#if !defined(apollo)
#include <unistd.h>
#endif
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <rpcsvc/ypclnt.h>

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

typedef struct Cyp_spec {
    char name[10];
    char account[9];
    int first;
} Cyp_spec_t;
static int Cyp_key = -1;
#define name Cyp->name
#define account Cyp->account
#define first Cyp->first

static int ypcall(instatus,inkey,inkeylen,inval,invallen,indata)
    int instatus ;
    char * inkey ;
    int inkeylen ;
    char * inval ;
    int invallen ;
    char *indata ;
{
    Cyp_spec_t *Cyp;
    char          * cp ; 
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

    /*
     * There is an error.
     */
    if ( instatus != 1 ) {
        *indata= '\0' ;
        return 1 ;
    }

    Cglobals_get(&Cyp_key,&Cyp,sizeof(Cyp_spec_t));
    /*
     * It is the first time.
     */
    if ( first == 0 ) {
        (void) strcpy(name,strtok(indata,":")) ;
        (void) strcpy(account,strtok((char *)NULL,":")) ;
        first= 1 ; 
    }
    
    cp= strtok(inval,":") ;
    if ( strcmp(cp,name) ) {
        /*
         * Username does not match.
         */
        return 0 ;
    }
    else    {
        cp= strtok((char *)NULL,":") ; 
        if ( *account == '*' ) {
            (void) strcpy(indata,cp) ;
            return 1 ;
        } 
        else    {
            if ( strcmp(cp,account) )
                return 0 ;
            (void) strcpy(indata,cp) ; 
            return 1 ;
        }
    }   
}
#undef name
#undef account
#undef first

char * ypgetacct(pwd,account,buffer)
    struct passwd * pwd ; 
    char      * account ;
    char      *  buffer ;
{
    struct ypall_callback call ;
    char              * domain ; 
    
    /*
     * Consulting YP.
     */
    if ( yp_get_default_domain(&domain) ) 
        return NULL ;

    (void) sprintf(buffer,"%s:%s:",pwd->pw_name,(account==NULL)?"*":account) ;
    call.foreach= ypcall ;
    call.data= buffer ;
    if ( yp_all(domain,"account",&call) ) 
        return NULL ;
    return ( * (char *) call.data == '\0' ) ? NULL : buffer ;
}
#endif /* NIS */
