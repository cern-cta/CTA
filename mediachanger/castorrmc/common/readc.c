/*
 * Copyright (C) 1990-1997 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)readc.c	1.7 12/13/97 CERN CN-SW/DC Antoine Trannoy";
#endif /* not lint */

/* readc.c              Fortran callable C I/O functions                */

/*
 * C functions called by Fortran READ -
 *
 * stream:	stream identifier.
 * buff  :	buffer address
 * count :	On entry, size of the buffer
 *		On exit, the number of bytes actually transferred.
 * status:	Status of the read.
 *
 *
 * read_s() called to read files with control words.
 * read_d() called to read files without control words.
 *
 */

#if !defined(CRAY)
#include <stdio.h>
#include <sys/types.h>

#if defined(hpux) || (defined(_AIX) && defined(_IBMR2))
void read_s(stream,buff,count,status)
#else
#if defined(_WIN32)
void _stdcall READ_S(stream,buff,count,status)
#else
void read_s_(stream,buff,count,status)
#endif
#endif	/* hpux	|| AIX */

#if defined(apollo)
        int * stream ;
#else
	FILE **stream ;
#endif  /* apollo */

        char *  buff ;
        int  * count ;
        int * status ;
{
	int rcode ;
        int size ;

	/*
 	 * Initialization
	 */
	*status= 0 ;

	/*
 	 * Reading the record length.
	 */	
#if defined(apollo)
        rcode= read(*stream,&size,sizeof(size)) ;
#else
	rcode= fread(&size,sizeof(size),1,*stream) ;
#endif  /* apollo */
	if ( rcode == 0 ) {
		/*
		 * End of file.
		 */
		*count = 0 ; 
		*status= 2 ;
		return ;
	}

#if defined(apollo)
	else if ( rcode != sizeof(size) ) {
#else
	else if ( rcode != 1 ) {
#endif
		/*
		 * Error.
		 */
		*status= 5 ; 
		return ;
	}
	/*
	 * Reading the record.
	 */
        if ( *count > size ) {
#if defined(apollo)
                rcode= read(*stream,buff,size) ; 
		if ( rcode != size ) {
#else
                rcode= fread(buff,size,1,*stream) ; 
		if ( rcode != 1 ) {
#endif  /* apollo */
			*status= 5 ; 
			return ; 
		}
		/*
		 * Number of bytes read.
		 */
#if defined(apollo)
		*count= rcode ; 
#else
		*count= size ;
#endif  /* apollo */
	}
	else {

#if defined(apollo)
                rcode= read(*stream,buff,*count) ; 
		if ( rcode != *count ) {
#else
                rcode= fread(buff,*count,1,*stream) ; 
		if ( rcode != 1 ) {
#endif  /* apollo */
			*status= 5 ; 
			return ; 
		}
	}


	/*
	 * Skipping the record's end if it 
	 * was only partially read.
	 */
	if ( *count < size ) {

		*status= -1 ; 
#if defined(apollo)
		if ( lseek(*stream,size-(*count),L_INCR) == -1 ) {
#else
		if ( fseek(*stream,size-(*count),1) == -1 ) {
#endif  /* apollo */
			*status= 5 ; 
			return ;
		}	
	}

	/*
	 * Reading the trailing record length. 
	 */
#if defined(apollo)
        rcode= read(*stream,&size,sizeof(size)) ;
	if ( rcode != sizeof(size) ) {
#else
	rcode= fread(&size,sizeof(size),1,*stream) ;
	if ( rcode != 1 ) {
#endif  /* apollo */
		*status= 5 ; 
		return ; 
	}
}

#if defined(hpux) || (defined(_AIX) && defined(_IBMR2))
void read_d(stream,buff,count,status)
#else
#if defined(_WIN32)
void _stdcall READ_D(stream,buff,count,status)
#else
void read_d_(stream,buff,count,status)
#endif
#endif	/* hpux	|| AIX */

#if defined(apollo)
        int * stream ;
#else
	FILE **stream ;
#endif  /* apollo */
        char *  buff ;
        int  * count ;
        int * status ;
{
	int rcode ;

 	/*
	 * Initialization.
	 */
	*status= 0 ; 

	/*
	 * Reading the record.
	 */
#if defined(apollo)
        rcode= read(*stream,buff,*count) ; 
#else
	rcode= fread(buff,*count,1,*stream) ;
#endif  /* apollo */
	if ( rcode == 0 ) {
		/*
		 * End of file.
		 */
		*count = 0 ;
		*status= 2 ; 
		return ;
	}

#if defined(apollo)
	else if ( rcode < 0 || rcode != *count ) {
#else
	else if ( rcode < 0 || rcode != 1 ) {
#endif  /* apollo */
		/*
		 * Error
		 */
		*status= 5 ; 
		return ;
	} 

}
#endif /* ! CRAY */
