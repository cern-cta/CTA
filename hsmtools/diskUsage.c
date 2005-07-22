/*
 * freespace.c,v 1.3 2003/11/14 11:19:03 jdurand Exp
 */

#ifndef lint
static char sccsid[] = "freespace.c,v 1.3 2003/11/14 11:19:03 CERN IT-ADC/CA Jean-Damien Durand";
#endif /* not lint */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include "rm_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "osdep.h"
#include "u64subr.h"

extern	char	*getconfent();
void usage _PROTO((char *));

static int help_flag = 0;
static int all_flag = 0;
static int host_flag = 0;
static int port_flag = 0;

u_signed64 g_total=0;
u_signed64 g_free=0;
u_signed64 g_disabled=0;
u_signed64 g_disabled_free=0;
int summary_mode=0;

static struct Coptions longopts[] = {
  {"help", NO_ARGUMENT,       &help_flag, 'h'},
  {"all",  NO_ARGUMENT,       &all_flag,  1},
  {"host", REQUIRED_ARGUMENT, &host_flag, 1},
  {"port", REQUIRED_ARGUMENT, &port_flag, 1},
  {NULL,   0,                 NULL,       0}
};

void
printnode_freespace(struct rmnode *node) {
  int i;
  char tmpbuf1[21];
  char tmpbuf2[21];
  for(i=0; i<node->nfs; i++) {
    if (summary_mode) {
      if (strcmp(node->fs[i].state, "Available")==0) {
        g_total += node->fs[i].totalspace;
        g_free  += node->fs[i].freespace;
      } else {
        g_disabled +=  node->fs[i].totalspace;
        g_disabled_free +=  node->fs[i].freespace;
      }
    }

    printf("%s\t%d\t%30s\t%15s\t\t%8s\t%8s\t%6.2f%% free\n",
           node->host,
           (int)node->currenttask,
           node->fs[i].name,
           node->fs[i].state,
           u64tostru(node->fs[i].freespace, tmpbuf1, 0),
           u64tostru(node->fs[i].totalspace, tmpbuf2, 0),
           ((double)node->fs[i].freespace)/
           ((double)node->fs[i].totalspace)*100.0);
  }
}

void
printsummary_freespace() {
  char tmpbuf1[21];
  char tmpbuf2[21];
  printf("\n");
  printf("Summary of Disabled filesystems  - Free: %8s\tTotal: %8s\t%6.2f%% free\n",
         u64tostru(g_disabled_free, tmpbuf1, 0),
         u64tostru(g_disabled, tmpbuf2, 0),
         ((double)g_disabled_free)/((double)g_disabled)*100.0);

  printf("Summary of Available filesystems - Free: %8s\tTotal: %8s\t%6.2f%% free\n",
         u64tostru(g_free, tmpbuf1, 0),
         u64tostru(g_total, tmpbuf2, 0),
         ((double)g_free)/((double)g_total)*100.0);

  printf("%6.2f%% of the total space is disabled\n", (((double)g_disabled)*100)/
         ( (double)g_total + (double)g_disabled ) );
}


int main(int argc, char *argv[]) {
  int c;
  int errflg = 0;
  char *host = NULL;
  int port = -1;
  char *func;
  struct rmnode rmnode;
  int nrmnode_out;
  struct rmnode *rmnode_out;
  u_signed64 flags = 0;

  func = argv[0];
  g_total = 0;
  g_free = 0;
  memset(&rmnode,'\0',sizeof(rmnode));

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "sh", longopts, NULL)) != -1) {
    switch (c) {
    case 0:
      if (host_flag && ! host) {
        host = Coptarg;
      } else if (port_flag && port < 0) {
        port = atoi(Coptarg);
      }
      break;
    case 's':
      summary_mode = 1;
      break;
    case 'h':
      help_flag = 1;
      break;
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }

  if (help_flag != 0) {
    usage (func);
    exit(0);
  }

  if (errflg != 0) {
    usage (func);
    exit(EXIT_FAILURE);
  }

  // Check we have a valid host
	if (NULL == host) {
    host = getconfent("RMMASTER", "HOST", 1);
    if (NULL == host) {
      printf("No host given for rmmaster and none found in config files.\nGiving up\n");
      exit(EXIT_FAILURE);
    }
  }

  if (all_flag) {
    flags |= RM_ALL;
  }

  c = rm_getnodes(host,port,flags, &rmnode, &nrmnode_out, &rmnode_out);

  if (c != 0) {
    fprintf(stderr,"%s error: %s\n", func, sstrerror(serrno));
    exit(EXIT_FAILURE);
  } else {
    if (nrmnode_out > 0) {
      int i;
      for (i = 0; i < nrmnode_out; i++) {
        printnode_freespace(rmnode_out+i);
      }
      rm_freenode(rmnode_out);
    } else {
      fprintf(stderr,"%s OK but nothing in return !?\n", func);
      exit(EXIT_FAILURE);
    }
  }
  if (summary_mode) {
    printsummary_freespace();
  }

  exit(EXIT_SUCCESS);
}

void usage(cmd)
     char *cmd;
{
  fprintf(stderr,"\nusage: %s [options] where options are:\n\n%s",
          cmd,
          "--host              rmmaster host\n"
          "--port              rmmaster port\n"
          "-s                  Displays Available FS summary\n"
          "-h                  Displays this help\n"
          "\n"
          );
}

