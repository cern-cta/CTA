#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "serrno.h"
#include "vmgr_api.h"

main(argc, argv)
     int argc;
     char **argv;
{
  char buf[80];
  char *density;
  char *dgn;
  char *dp;
  int errflg = 0;
  char *lbltype;
  char *manufacturer;
  char *media_letter;
  char *model;
  char *p;
  char *poolname;
  char *sn;
  char *vid;
  char *vsn;
  int status;

  if (argc != 12) {
    fprintf (stderr,
             "usage: vmgrentertape vid vsn dgn density lbltype model media_letter manufacturer sn poolname\n"
             "\n"
             "  where vid              Volume ID               ex: Y31100\n"
             "        vsn              Volume SN               ex: Y31100\n"
             "        dgn              Device Group Name       ex: SD3R3\n"
             "        density          Volume Density          ex: 50GC\n"
             "        lbltype          Volume Label Type       ex: al\n"
             "        model            Volume Model            ex: SD3\n"
             "        media_letter     Associated Media Letter ex: C (can be \"\")\n"
             "        manufacturer     Manufacturer            ex: STK (can be \"\")\n"
             "        sn               Volume Serial Number    ex: 1234567890 (can be \"\")\n"
             "        poolname         Associated Poolname     ex: Public (can be \"\")\n"
             "        status           Associated status       ex: 0\n"
             "\n"
             "Comments to the CASTOR Developpment Team <castor-dev@listbox.cern.ch>\n"
             "\n");
    return(EXIT_FAILURE);
  }

  vid = argv[1];
  vsn = argv[2];
  dgn = argv[3];
  density = argv[4];
  lbltype = argv[5];
  model = argv[6];
  media_letter = argv[7];
  manufacturer = argv[8];
  sn = argv[9];
  poolname = argv[10];
  status = atoi(argv[11]);

  if (vmgr_entertape (vid, NULL, dgn, density, lbltype, model,
                      media_letter, manufacturer, sn, poolname,status)) {
    fprintf (stderr, "%s: %s\n", vid, sstrerror(serrno));
    return(EXIT_FAILURE);
  }

  printf("--> OK\n");

  return(EXIT_SUCCESS);
}
