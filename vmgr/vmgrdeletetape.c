/*
 * Copyright (C) 2000-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	vmgrdeletetape - delete a tape volume */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string.h>
#include <unistd.h>
#include "Cns_api.h"
#include "serrno.h"
#include "vmgr_api.h"
extern	char	*optarg;
extern	int	optind;


/**
 * Connects to the specified name server host and counts the number of enabled
 * and disabled files still on the tape with the specified VID.
 *
 * @param vid             The VID of the tape.
 * @param nbEnabledFiles  Output parameter: The number of enabled files on the
 *                        specified tape.
 * @param nbLDeleteFiles  Output parameter: The number of disableds files on the
 *                        specified tape.
 * @param errorBuf        If failure then this buffer will be filled with an
 *                        error message.
 * @param errorBufLen     The length of the errorBuf in bytes.
 * @return                0 on success and -1 on failure.
 */
int countNsFilesOnTape(char *vid, unsigned int *nbActiveFiles,
                       unsigned *nbLDeleteFiles, char *errorBuf, const size_t errorBufLen) {

  struct Cns_direntape *dtp;
  Cns_list list;
  int flags;

  flags = CNS_LIST_BEGIN;
  while ((dtp = Cns_listtape (NULL, vid, flags, &list, 0)) != NULL) {
    flags = CNS_LIST_CONTINUE;
    if (dtp->s_status == 'D') {
      (*nbLDeleteFiles)++;
    } else {
      (*nbActiveFiles)++;
    }
  }
  if (serrno > 0 && serrno != ENOENT) {
    if (errorBufLen >= strlen( sstrerror(serrno))){
      strcat(errorBuf, sstrerror(serrno));
    } else {
      strncat(errorBuf, sstrerror(serrno), errorBufLen);
    }
    return(-1);
  } 
  if (serrno == 0) {
    (void) Cns_listtape (NULL, vid, CNS_LIST_END, &list, 0);
  }

  /* Implement by passing NULL as the NS host to Cns_listtape                */
  /* Copy an modify the code already existing at trunk/hsmtools/reclaim.c:97 */
  /* Once finished, copy this code into trunk/hsmtools/reclaim.c and use it  */
  /* there.                                                                  */
  return 0;
}


int main(int argc, char **argv) {
  int  c             = 0;
  int  nbParseErrors = 0;
  char *nsHost       = NULL;
  char *vid          = NULL;

  /* Parse the command-line */
  while((c = getopt (argc, argv, "h:V:")) != EOF) {
    switch (c) {
    case 'h':
      nsHost = optarg;
      break;
    case 'V':
      vid = optarg;
      break;
    case '?':
      nbParseErrors++;
      break;
    default:
      break;
    }
  }
  if(optind < argc) {
    nbParseErrors++;
    fprintf(stderr, 
            "%s Error: Unknown non-option command-line argument(s):", 
            argv[0]);

    while(optind < argc) {
      fprintf(stderr, " %s",  argv[optind++]);
    }
    fprintf(stderr, "\n");
  }
  if(vid == NULL) {
    nbParseErrors++;
    fprintf(stderr, "%s Error: The vid has not been specified\n",
            argv[0]);
  }

  /* Display the usage message if there were one or more parse errors */
  if(nbParseErrors) {
    fprintf (stderr, "usage: %s [-h name_server] -V vid\n", argv[0]);
    return(USERR);
  }

  /* Set the number of consistency errors to zero */
  int consistencyError = 0, 
    nbTapeFiles = 0, 
    nbNsFiles = 0;

  /* Query the VMGR about the tape in order to determine the number of tape */
  /* files on the tape                                                      */
  struct vmgr_tape_info_byte_u64 tape_info;
  if (vmgr_querytape_byte_u64 (vid, 0, &tape_info, NULL) < 0) {
    fprintf (stderr, "%s %s: %s\n", argv[0], vid,
             (serrno == ENOENT) ? "No such volume" : sstrerror(serrno));
    exit (USERR);
  }
  nbTapeFiles = tape_info.nbfiles;

  /* Display a "tape not empty" error and increment the number of */
  /* consistency errors                                           */
  if(nbTapeFiles > 0){
    fprintf(stderr, 
            "%s Error: %s contains tape files. nbFiles: %d\n", 
            argv[0], vid, nbTapeFiles);
    consistencyError++;
  }

  /* Get the number of active segments and the number of logically deleted   */
  /* segmenton the tape that are still in the CASTOR namespace from the name */
  /* server */ 
  unsigned int nbDisableFiles = 0, nbActiveFiles = 0;
  const size_t STRERRORBUFLEN = 256;
  char errorBuf[STRERRORBUFLEN];
  int rc = countNsFilesOnTape(vid, &nbActiveFiles, &nbDisableFiles, errorBuf, STRERRORBUFLEN);
  if (rc==0){
    nbNsFiles = nbDisableFiles + nbActiveFiles;
    
    if(nbActiveFiles > 0){
      fprintf(stderr,  
              "%s Error: %s contains active CASTOR files. nbFiles: %d\n", 
              argv[0], vid, nbActiveFiles);
      consistencyError++;
    }
    if(nbDisableFiles > 0){
      fprintf(stderr, 
              "%s Error: %s contains logically deleted CASTOR files. nbFiles: %d\n", 
              argv[0], vid, nbDisableFiles);
      consistencyError++;
    }
  } else {
    fprintf(stderr, "%s Error: %s\n",argv[0], errorBuf);
  }
 
  /* Display a "vmgr out of sync with name server" error and increment the   */
  /* number of consistency errors if the total number of files in the CASTOR */
  /* namespace (enabled + disabled) is greater than the number of tape files */
  /* errors if the  tape constains tape files                                */
  if (nbNsFiles > nbTapeFiles){
    fprintf(stderr, "%s Error: Vmgr out of sync with name server.\n", argv[0]);
    consistencyError++;
    exit (USERR);
  }

  /* If the number of consistency errors is greater than 0 then return       */
  if (consistencyError > 0){
    exit (USERR);
  }

  /* Delete the tape from the vmgr */
  if(vmgr_deletetape(vid) < 0) {
    fprintf(stderr, "%s %s:", argv[0], vid);

    if(serrno == ENOENT) {
      fprintf(stderr, "No such tape\n");
      return(USERR);
    } else {
      fprintf(stderr, "%s\n", sstrerror(serrno));
      return(serrno);
    }
  } else {
    printf("vmgrdeletetape succesfully deleted tape:\n%s %s %s %s %s %s %s\n",
           tape_info.vid,
           tape_info.library,
           tape_info.density,
           tape_info.lbltype,
           tape_info.model,
           tape_info.media_letter,
           tape_info.poolname);
  }

  return(0); /* Success */
}
