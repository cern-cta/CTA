/*
 * Copyright (C) 2000-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*	Ctape_devinfo - table of device characteristics */

#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"

static struct devinfo devinfo[] = {
  { "LTO",    1, 1, 1,-1, 1,    16777215, 262144, 0x10, { {D100G, 0x40}, {D100GC, 0x40}, {D200G,  0x42}, {D200GC, 0x42}, {D400G, 0x42}, {D400GC, 0x42}, {D800G, 0x42}, {D800GC, 0x42}, {0,         0} } }, 
  { "3592",   1, 1, 1, 1, 1,     2097152, 262144, 0x10, { {D300G,    0}, {D300GC,    0}, {D500G,     0}, {D500GC,    0}, {0,        0}, {0,         0}, {0,        0}, {0,         0}, {0,         0} } },
  { "T10000", 1, 1, 1, 0, 1,      262144, 262144, 0x10, { {D500G,    0}, {D500GC,    0}, {0,         0}, {0,         0}, {0,        0}, {0,         0}, {0,        0}, {0,         0}, {0,         0} } },
  /* new devices should be added here */
  
  /* last line corresponds to unknown device */

  { "",       1, 2, 0, -1, 1,     262144,  32760, 0x00, { {0,        0}, {0,         0}, {0,         0}, {0,         0}, {0,        0}, {0,         0}, {0,        0}, {0,         0}, {0,         0} } }
};

struct devinfo * Ctape_devinfo(const char *const devtype)
{
	char devtype_local[CA_MAXMODELLEN+1];
	int i;

	strcpy (devtype_local, devtype);
	for (i = 0; *devinfo[i].devtype; i++)
		if (strstr (devtype_local, devinfo[i].devtype) == devtype_local) break;
	return (&devinfo[i]);
}

/**
 * This function checks if the specified device type is in the list of
 * supported types.
 *
 * @param  deviceType       A pointer to the '\0' terminated string to check.
 * @return 1 if the device type is supported else 0.
 */
int deviceTypeIsSupported(const char *const deviceType) {
  int i;
  for (i = 0; *devinfo[i].devtype; i++) {
    if (0 == strncmp(deviceType, devinfo[i].devtype,
                     strlen(devinfo[i].devtype)))
      return 1;
    }
  return 0;
}

