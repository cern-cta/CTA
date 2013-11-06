/*
 * Copyright (C) 2000-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*	Ctape_devinfo - table of device characteristics */

#include <string.h>
#include <sys/types.h>
#include "h/Ctape.h"
#include "h/Ctape_api.h"

static struct devinfo devinfo[] = {
  { "LTO" }, 
  { "3592" },
  { "T10000" },
  /* new device types should be added here */
  
  { "" }
};

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

