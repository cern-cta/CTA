/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

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

#include <stdio.h>
#include <sys/types.h>
#include <osdep.h>

void read_s_(FILE **stream,
             char *  buff,
             int  * count,
             int * status)
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
	rcode= fread(&size,sizeof(size),1,*stream) ;
	if ( rcode == 0 ) {
		/*
		 * End of file.
		 */
		*count = 0 ; 
		*status= 2 ;
		return ;
	}

	else if ( rcode != 1 ) {
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
                rcode= fread(buff,size,1,*stream) ; 
		if ( rcode != 1 ) {
			*status= 5 ; 
			return ; 
		}
		/*
		 * Number of bytes read.
		 */
		*count= size ;
	}
	else {
    rcode= fread(buff,*count,1,*stream) ; 
		if ( rcode != 1 ) {
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
		if ( fseek(*stream,size-(*count),1) == -1 ) {
			*status= 5 ; 
			return ;
		}	
	}

	/*
	 * Reading the trailing record length. 
	 */
	rcode= fread(&size,sizeof(size),1,*stream) ;
	if ( rcode != 1 ) {
		*status= 5 ; 
		return ; 
	}

}
void read_d_(FILE **stream,
             char *  buff,
             int  * count,
             int * status)
{
	int rcode ;

 	/*
	 * Initialization.
	 */
	*status= 0 ; 

	/*
	 * Reading the record.
	 */
	rcode= fread(buff,*count,1,*stream) ;
	if ( rcode == 0 ) {
		/*
		 * End of file.
		 */
		*count = 0 ;
		*status= 2 ; 
		return ;
	}

	else if ( rcode < 0 || rcode != 1 ) {
		/*
		 * Error
		 */
		*status= 5 ; 
		return ;
	} 

}
