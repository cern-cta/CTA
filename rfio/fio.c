/*
 * $Id: fio.c,v 1.2 1999/07/20 12:47:58 jdurand Exp $
 *
 * $Log: fio.c,v $
 * Revision 1.2  1999/07/20 12:47:58  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990-1997 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)fio.c	1.13 12/15/97 CERN CN-SW/DC F. Hassine";
#endif /* not lint */

/*      fortran.c    C callable Fortran I/O                     */

/*
 *  int usf_open(int *unit, char *file, int *append) returns irc
 *  int udf_open(int *unit, char *file, int *lrecl , int *trunc) returns irc
 *  int usf_write(int *unit, char *buf, int *nwrit) returns irc
 *  int udf_write(int *unit, char *buf, int *nrec, int *nwrit) returns irc
 *  int usf_read(int *unit, char *buf, int *nwant) returns irc
 *  int udf_read(int *unit, char *buf, int *nrec, int *nwant) returns irc
 *  int uf_close(int *unit) returns irc
 *  void uf_cread(int *unit, char *buf, int *nrec, int *nwant, int *ngot, int *irc)
 *
 *              unit = logical fortran unit;
 *              file = filename;
 *              filen= filename length, used only by fortran code;
 *              append= open mode "append" for file when append > 0;
 *              irc = error (errno) status if any occured, 0 otherwise;
 *              reclen= record length for files open in DIRECT mode.
 *              nrec= record number to be accessed.
 *
 * WARNING : uf_cread returns the following status values:
 * status = -1	a few bytes remain in the record
 * 	     0  EOR
 *	     2  EOF
 * 	     >=4 Error
 */
	
#define OPEN_MODE 0644
#define MAXFTNLUN 99

#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <serrno.h>

#if !defined(apollo)
#define SET SEEK_SET
#define CUR SEEK_CUR
#define END SEEK_END
#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif
#else /* apollo */
#define SET L_SET
#define CUR L_INCR
#define END L_XTND
#include <sys/file.h>
#endif /* apollo */

static int lun2fd[MAXFTNLUN];  /* Fortran logical units to file descr. mapping */

/*
 * Fortran logical unit record length (direct access). -1 for
 * sequential access. Initialized by usf_open(), udf_open()
 */

static int lun2reclen[MAXFTNLUN];
static int tested = 0 ;	/* Test is_usf() done ? */

extern int errno;
			 
int usf_open(unit, file,append,trunc)

int 	*unit;
char 	*file;
int 	*append;
int     *trunc;

{	int fd;
	int flags;
	int errno1;

	if (*unit>MAXFTNLUN)
		return (EBADF); 
	if (*unit <1)
		return (EINVAL);
	else {
		if (*append==0) {
			if (*trunc)
				flags= O_RDWR | O_CREAT | O_TRUNC;
			else
				flags= O_RDWR | O_CREAT;
			if ( (fd = open (file, flags , OPEN_MODE))<0 ) {
                                errno1=errno;
                                if  ( (fd = open (file, O_RDONLY, OPEN_MODE) )<0 ) {
                                        if (errno1==EACCES)
                                                return(errno1);
                                        else
                                                return(errno);
                                }
			}
			lun2fd[*unit-1]=fd;
			lun2reclen[*unit-1] = -1;
		
		}
		/*
		 * In append mode we do not take
		 * the value of trunc int account
		 */
		else {
			if( ( fd = open (file , O_RDWR | O_CREAT| O_APPEND , OPEN_MODE) ) <0) {
				return(errno);
			}
			else {	
				lun2fd[*unit-1]=fd;
                                lun2reclen[*unit-1] = -1;/* Not in Direct access */

			}
		}
	}
	return (0);
}

int udf_open(unit, file, lrecl,trunc)

int 	*unit;
char 	*file;
int 	*lrecl;
int	*trunc;

{
	int flags;
	int fd;
	int errno1;
	if ( (*unit>MAXFTNLUN) || (*unit<1) || (*lrecl<0)  || ( (*lrecl) % 8 ) )
		return(EBADF);
	else {
		if (*trunc) {
			flags= O_RDWR | O_CREAT | O_TRUNC;
		}
		else
			flags=  O_RDWR | O_CREAT ;
		if ( ( fd = open (file, flags , OPEN_MODE) )   <0 ) {
                        errno1=errno;
                        if ( (fd = open (file, O_RDONLY , OPEN_MODE)) <0 ) {
                                if ( errno1==EACCES )
                                        return(errno1);
                                else
                                        return(errno);
                        }
		}
		lun2fd[*unit-1]=fd;
		lun2reclen[(*unit) -1]= (*lrecl) ;
	}
	return (0);
}

int usf_write(unit, buf, nwrit)

int	*unit;
char	*buf;
int	*nwrit;

{	int fd,reclen;
	fd=lun2fd[*unit-1];
	reclen=lun2reclen[*unit-1];

	if (reclen != -1) {
		return(ENOENT);
	}
	else {
		if (*nwrit > 0) 
			write( fd, nwrit , sizeof(int) );
		if ( write ( fd , (char *)buf , *nwrit) <0 ) {
			return(errno);
		}
		if (*nwrit >0)
			write( fd, nwrit , sizeof(int) );
		return(0);
	}
}

int udf_write(unit, buf, nrec, nwrit)

int 	*unit;
char 	*buf;
int 	*nrec;
int 	*nwrit;

{	int fd;
	int reclen,i;
	int zero=0;

	fd=lun2fd[*unit-1];
	reclen=lun2reclen[*unit-1];
	if (reclen <=0 ) {
		return(ENOENT);
	}
	else {
		if (*nwrit <= reclen) {
				lseek(fd , (*nrec-1)*reclen , SET );
				if ( write ( fd , (char *)buf , *nwrit) <0 ){
					return(errno);
				}

				else {
					for (i= (*nwrit) +1; i <=reclen; i++) {
						 write ( fd , &zero , 1);
					}
					return(0);
				}
		}
		else { 
			return(EINVAL);
		}
	}
}


/*
 *	 usf_read performs the read for unformatted sequential files.  
 *	 irc is given the value 0 if no error occured,
 *	 and the C errno value otherwise.
 */

	
int usf_read(unit, buf, nwant)

int	*unit;
char	*buf;
int	*nwant;

{	int fd,reclen;
	int c,d;
	int got;

	fd=lun2fd[*unit-1];
	reclen=lun2reclen[*unit-1];

	if (reclen != -1) {
		*nwant=0;
		errno = ENOENT ;
		return(ENOENT);
	}	
	else {
		if ( (read(fd, &c , sizeof (int) ) ) <0 )  {
				*nwant=0;
				return(errno);
		}
		else {
			if (*nwant >=c) {
				*nwant=c;
				if ( (got=read(fd,buf,*nwant) )<0) {
					*nwant=0;
					return(errno);
				}
				else {
					*nwant=got;
				}
				
			}
			else {
				if ( (got=read(fd,buf,*nwant) )<0 ) {
					*nwant=0;
					return(errno);
				}
				else {
					*nwant=got;
				}
				lseek(fd, c-got, CUR);
			}
		}

	/* Checking that the following integer read is equal
	 * to the first one :
	 */

		if ( ( read( fd, &d , sizeof (int) ) <0 ) )   {
			return(errno);
		}
		else {
			if (c != d ) {
				serrno = SEBADFFORM ;
				return(ESPIPE) ; 
			}
				
		}
	return(0);
	}
}


int udf_read(unit, buf, nrec, nwant)

int 	*unit;
char 	*buf;
int 	*nrec;
int 	*nwant;

{	int fd=lun2fd[*unit-1];
	int reclen=lun2reclen[*unit-1];
	int got;

	if ((reclen<=0) ||(*unit>MAXFTNLUN) || (*unit<1) || (*nrec<0) || (*nwant < 0)) {
		*nwant=0;
		if (reclen<=0) {
			errno = ENOENT ;
			return(ENOENT);
		}
		else {
			errno = EINVAL ;
			return(EINVAL) ; 
		}
	}
	else {
		if (*nwant>reclen)  
				*nwant=reclen;
		lseek(fd , reclen*(*nrec-1) , SET ) ;
 		if ( ( got=read(fd, buf , *nwant) ) <0) {
				*nwant=0;
				return(errno);
		}
		else{
				*nwant=got;
				return(0);
		}
	}
}


int uf_close(unit)

int 	*unit;

{	
	if ( close( lun2fd[*unit-1] ) < 0 ) {
		return(errno);
	}
	else {
		lun2reclen[*unit-1] = -1;
		return(0);
	}

}	

/*
 *	 uf_cread replaces the frdc function
 *	 for all kinds of machines except CRAY.  
 */

void uf_cread(unit, buf, nrec, nwant, ngot, irc)

int 	*unit;
char 	*buf;
int 	*nrec;
int 	*nwant;
int	*ngot;
int 	*irc;

{
	int fd=lun2fd[*unit-1];
	int sequential=0;	 
	int reclen=lun2reclen[*unit-1];
	int len=0;
	int rcode=0;

/*
 *	detecting  wether the file is
 *		Sequentially
 *	or 	Directly
 *	accessed in fortran
 */

	if (*nwant == 0) {
		*ngot = 0;
		*irc= -1 ; /*Still to read */
		return ;
	}
	if (reclen<0) 
		sequential=1; 	/* The file is sequential */

	if (!sequential) { 	/* Direct access */

		if (*nwant > reclen)
			*nwant=reclen;
		lseek(fd, (*nrec -1)*reclen, SET);
		rcode=read(fd,buf, (*nwant) );
		if (rcode<0) {
			(*irc)=5; /* Error */
			(*ngot)=0;
		}
		else {
			if ( (rcode >=0 ) && (rcode < *nwant) )
				(*irc)=2; /* EOF reached */
			else if ( (rcode == *nwant) && (*nwant==reclen) )
				(*irc)=0; /* EOR reached */
			else if (  (rcode == *nwant) && (*nwant < reclen) )
				(*irc)= -1; /* There is still bytes to read */
			(*ngot)=rcode;
		}
	}
	else {
		/* Sequential access 
		 */
		int rrc =0 ;
		if (!tested) {
		    if ( (rrc = is_usf(fd)) == 0 || rrc == 3 ) {
			*irc = SEBADFFORM  ;
			*ngot = 0 ;
			return ;
		    }
		    tested ++ ;
		}
		rcode=read(fd,&len, sizeof(int) );
		if (rcode <0) {
			(*irc)=5; /* Error */
			(*ngot)=0;
		}
		else if (rcode==0){
			(*irc)=2; /* EOF reached */
			(*ngot)=0;
		}
		else {
			if ( len <*nwant ) { *ngot=len;
					     *nwant=len;
			}
			if ( (rcode=read(fd, buf, *nwant) ) <0) {
					*irc=5; /* Error */
					*ngot=0;
			}
			else {
					*ngot=rcode;
					if (rcode == 0)
						*irc=2 ; /* EOF */
					else if (rcode < *nwant) 
						*irc=2 ; /* EOF */
					else if ( (rcode== *nwant) && (len > *nwant) )
						(*irc)= -1 ; /* Still to read */
					else if  ((rcode == *nwant) && (len == *nwant) )
						(*irc)=0 ; /* EOR */ 
				
				/* Skipping the end of record if 
				 * necessary, and also the length 
				 * integer.
				 */
					lseek(fd,len-(*ngot)+sizeof(int), CUR);
			}
		}
	}
}

/*
 * is the disk file an unformatted sequential file ?
 * returns -1 on failure,
 *         0  if not sequential file
 *         1  if file may be  unformatted sequential
 *         2  file is empty
 *         3  first record is null
 * It leaves the file pointer at the place it was.
 *
 */
int is_usf( fd )
int fd ;
{
        int len,llen ;
        int rcode,rc ;
	int curr ;

	curr = lseek(fd,0,CUR) ;
	if (curr > 0)
		lseek(fd,0, SET) ;
	if (curr < 0)
		return -1 ;
        if ( (rcode = read(fd,&len,sizeof(int))) < 0 )
                return -1 ;
        if (rcode == 0 ) {
                lseek(fd,curr,SET) ;
                return 2 ;
        }
        if ( len < 0 ) {
                lseek(fd,curr,SET) ;
                return 0 ;
        }
        if (len == 0){
                lseek(fd,curr,SET) ;
                return 3 ;
        }
        else {
                rcode=lseek(fd,len, CUR) ;
                if (rcode < 0) {
                        lseek(fd,curr,SET) ;
                        return 0 ;
                }
                rc=read(fd,&llen,sizeof(int));
                if (rc < sizeof(int)) {
                        /* We need at least 1 record to decide
                         * it is sequential
                         */
                        lseek(fd,curr,SET) ;
                        return 0 ;
                }
                if (llen != len) {
                        lseek(fd,curr,SET) ;
                        return 0 ;
                }
                else {
                        lseek(fd,curr,SET) ;
                        return 1 ;
                }
        }
}

