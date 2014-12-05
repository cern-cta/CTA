/*
 * $Id: Cupv_struct.h,v 1.3 2002/06/07 15:56:17 bcouturi Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */
 
#pragma once
 
#include "h/Castor_limits.h"

struct Cupv_userpriv {
  int uid;
  int gid;
  char srchost[CA_MAXREGEXPLEN + 1];
  char tgthost[CA_MAXREGEXPLEN + 1];
  int privcat;
};








