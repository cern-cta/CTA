/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* ypgetacct()              Getting account id in YP      */

#if defined(NIS)
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <rpcsvc/ypclnt.h>
#include "osdep.h"

#define strtok(X,Y) strtok_r(X,Y,&last)

typedef struct Cyp_spec {
    char _name[10];
    char _account[9];
    int _first;
} Cyp_spec_t;
static int Cyp_key = -1;
#define name Cyp->_name
#define account Cyp->_account
#define first Cyp->_first

static int ypcall(int instatus,
                  char * inkey,
                  int inkeylen,
                  char * inval,
                  int invallen,
                  char *indata)
{
    Cyp_spec_t *Cyp;
    char          * cp ; 
    char *last = NULL;

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

char *ypgetacct(struct passwd * pwd, 
                char      * account,
                char      *  buffer)
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
