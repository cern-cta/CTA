/*
 * $Id: stgpath_Cdbtest.c,v 1.5 2000/06/06 10:13:12 jdurand Exp $
 */

/* ============== */
/* System headers */
/* ============== */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <serrno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

/* ============= */
/* Local headers */
/* ============= */
#include "Cdb_api.h"
#include "stage.h"
#include "stgdb_Cdb_ifce.h"
#include "serrno.h"           /* See CASTOR/h            */
#include "Cdb_api.h"

/* ====== */
/* Macros */
/* ====== */
#define LOGIN "TOTO"
#define PASSWORD "TOTO"
#define INSERT 1
#define DELETE 2

#ifdef LINE_PAGESIZE
#undef LINE_PAGESIZE
#endif
#define LINE_PAGESIZE 256        /* Default line paging size (a-la-gnu)     */

/* ========== */
/* Prototypes */
/* ========== */
int _logfile_readline _PROTO((FILE *, char **, size_t *));

/* ================ */
/* Global variables */
/* ================ */
int reqid;

void _stgpath_Cdbtest_usage _PROTO(());

int main(argc,argv)
		 int argc;
		 char **argv;
{
	struct stgpath_entry stpp;
	char *Default_db_user = "Cstg_username";
	char *Default_db_pwd  = "Cstg_password";
	struct stgdb_fd dbfd;
	FILE *fd;
	int line_number;
	int i;
	char        *line           = NULL; /* Line buffer                         */
	size_t       size           = 0;    /* Line buffer size                    */
	int last_command;
	char *p;
	int ip;
	char *found;
    int c;
    extern char *optarg;
    extern int optind;
    extern int opterr;
    extern int optopt;
    int errflg = 0;
    int max = -1;
    int imax = 0;

    while ((c = getopt(argc,argv,"hn:")) != EOF) {
      switch (c) {
      case 'h':
        _stgpath_Cdbtest_usage();
        return(EXIT_SUCCESS);
      case 'n':
        max = atoi(optarg);
        break;
      case '?':
        ++errflg;
        fprintf(stderr,"Unknown option\n"); fflush(stderr);
        break;
      default:
        ++errflg;
        fprintf(stderr,"?? getopt returned character code 0%o (octal) 0x%lx (hex) %d (int) '%c' (char) ?\n"
                ,c,(unsigned long) c,c,(char) c);  fflush(stderr);
        break;
      }
      if (errflg != 0) {
        fprintf(stderr,"### getopt error\n"); fflush(stderr);
        _stgpath_Cdbtest_usage();
        return(EXIT_FAILURE);
      }
    }

	if (argc < 2) {
      _stgpath_Cdbtest_usage();
      return(EXIT_FAILURE);
    }
	strcpy(dbfd.username,Default_db_user);
	strcpy(dbfd.password,Default_db_pwd);

	/* Log to the database server */
	if (stgdb_login(&dbfd) != 0) {
		stglogit("stgdb_login", STG100, "login", sstrerror(serrno), __FILE__, __LINE__);
		return(EXIT_FAILURE);
	}

	/* Open the database */
	if (stgdb_open(&dbfd,"stage") != 0) {
		stglogit("stgdb_open", STG100, "open", sstrerror(serrno), __FILE__, __LINE__);
		return(EXIT_FAILURE);
	}

	/* Loop on logfiles */
	for (i = optind; i < argc; i++) {

		/* Try to open logfile */
		if ((fd = fopen(argv[i],"r")) == NULL) {
			fprintf(stderr,"### error opening file %s, %s\n",argv[1],strerror(errno));
			continue;
		}

		/* Read the logfile line per line */
		line_number = 0;

		for ( ; ; ) {
			if (_logfile_readline(fd, &line, &size) != 0)
				break;

			++line_number;

			/* Select only those containing STG93 or STG98 */
			if ((found = strstr(line,"STG93 - removing link ")) != NULL) {
				last_command = DELETE;
			} else if ((found = strstr(line,"STG98 - /usr/local/bin/stagein ")) != NULL) {
				last_command = INSERT;
			} else {
				continue;
			}

			/* Get all the links */
			/* We consider they are all of the form: <hostname:path> */
			ip = 0;
			if ((p = strtok(line," ")) != NULL) {
				do {
					ip++;
					if (p > found) {
						/* Search for the ':' separator */
						if (strstr(p,":") != NULL) {
							char *end;
							char save;

							/* Search the end of the link name, e.g. a blank or a newline or EOL */
							if ((end = strchr(p,' ')) != NULL ||
									(end = strchr(p,'\n')) != NULL ||
									(p[strlen(p)] == '\0')) {
								if (p[strlen(p)] == '\0') {
									end = &(p[strlen(p)]);
								}
								save = end[0];
								end[0] = '\0';
                                if (max >=0 && ++imax > max) {
                                  return(EXIT_SUCCESS);
                                }
								switch (last_command) {
								case INSERT:
									fprintf(stdout,"[%s] INSERT %10d %s\n",argv[i],reqid,p);
									stpp.reqid = reqid;
									strcpy(stpp.upath,p);
									if (stgdb_ins_stgpath(&dbfd,&stpp) != 0) {
										fprintf(stderr,"--> [%s] INSERT ERROR (%s)\n",argv[i],sstrerror(serrno));
										if (serrno == EDB_D_CORRUPT) {
											exit(EXIT_FAILURE);
										}
									}
									break;
								case DELETE:
									fprintf(stdout,"[%s] DELETE %10d %s\n",argv[i],reqid,p);
									stpp.reqid = reqid;
									strcpy(stpp.upath,p);
                                    if (Cdb_keyfind_fetch(&(dbfd.Cdb_db),
                                                          "stgcat_link",
                                                          "stgcat_link_per_upath",
                                                          NULL,
                                                          &stpp,
                                                          NULL,
                                                          &stpp) != 0) {
                                      fprintf(stderr,"--> [%s] Cdb_keyfind_fetch error (%s)\n",argv[i],sstrerror(serrno));
                                      if (serrno == EDB_D_CORRUPT) {
                                        exit(EXIT_FAILURE);
                                      }
                                    } else {
                                      if (stgdb_del_stgpath(&dbfd,&stpp) != 0) {
										fprintf(stderr,"--> [%s] DELETE ERROR (%s)\n",argv[i],sstrerror(serrno));
										if (serrno == EDB_D_CORRUPT) {
                                          exit(EXIT_FAILURE);
										}
                                      }
                                    }
									break;
								default:
									fprintf(stderr,"### Unknown last_command = %d\n",last_command);
									return(EXIT_FAILURE);
								}
								end[0] = save;
							}
						}
					} else {
						/* Is it the reqid ? */
						if (ip == 3) {
							reqid = atoi(p);
						}
					}
					p = strtok(NULL," ");
				} while (p != NULL);
			}
		}

		fclose(fd);
	}

	if (line != NULL) {
		free(line);
	}

	/* Close the database */
	if (stgdb_close(&dbfd) != 0) {
		stglogit("stgdb_close", STG100, "close", sstrerror(serrno), __FILE__, __LINE__);
		return(EXIT_FAILURE);
	}

	/* Logout the database server */
	if (stgdb_logout(&dbfd) != 0) {
		stglogit("stgdb_logout", STG100, "logout", sstrerror(serrno), __FILE__, __LINE__);
		return(EXIT_FAILURE);
	}

	return(EXIT_SUCCESS);
}

/* ----------------------------------------
 * _logfile_readline utility
 *
 * Parameters: FILE   *fd    stream          (input)
 *             char  **buf   buffer          (output)
 *             size_t *size  buffer_size     (output)
 *
 * Return value: 0 (OK) or -1 (ERROR) or 1 (EOF)
 *
 * ---------------------------------------- */
int _logfile_readline(fd, buf, size)
		 FILE   *fd;
		 char  **buf;
		 size_t *size;
{
	int n = 0;                              /* Number of read characters */
	int c;                                  /* Last read character       */

	if (fd == NULL || buf == NULL || size == NULL) {
		serrno = EINVAL;
		return(-1);
	}

	if (*size <= 0) {
		/* Buffer not yet initalized */
		if ((*buf = (char *) malloc(LINE_PAGESIZE)) == NULL) {
			serrno = SEINTERNAL;
			return(-1);
		}
		*size = LINE_PAGESIZE;
	}

	/* We read up to '\n' or EOF, maximum (*size) bytes */
	for ( ; ; ) {

		/* We read character per character */
		/* (using the library function, not the macro */
		c = (getc)(fd);

		/* We exit if end-of-file or if separator found */
		if (c == EOF || c == '\n' || c == 10)
			break;
		
		if ((size_t) (n+1) > *size) {
			/* We know we will exceed buffer space */
			char *dummy = NULL;
			/* So we try to allocate it to its double size */
			if ((dummy = (char *) realloc(*buf,2 * (*size))) == NULL) {
				free(*buf);
				*buf = NULL;
				*size = 0;
				serrno = SEINTERNAL;
				return(-1);
			}
			/* Now, we have more space */
			*buf = dummy;
			*size *= 2;
		}
		
		/* We don't want '\0' characters... */
		if (c == '\0')
			c = ' ';

		/* We store result */
		(*buf)[n++] = c;
	}
	
	/* We make sure that null terminates */
	(*buf)[n] = '\0';

	return(c == EOF ? ( n > 0 ? 0 : 1 ) : 0);
}

void _stgpath_Cdbtest_usage() {
  fprintf(stderr,
          "Usage: stgpath_Cdbinsert <logfile> [<logfile> ...]\n"
          "\n"
          "Nota: Cdb server and port is setted via the environment variables\n"
          "      $CDB_HOST and $CDB_SERV, respectively.\n"
          "\n"
          "Author: Jean-Damien.Durand@cern.ch\n"
          );
}
