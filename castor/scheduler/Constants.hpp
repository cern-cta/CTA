/******************************************************************************
 *                      Constants.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: Constants.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/03/25 10:28:55 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef SCHEDULER_CONSTANTS_HPP
#define SCHEDULER_CONSTANTS_HPP 1

// Include files
#include "castor/Constants.hpp"
#include "osdep.h"

extern "C" {
  #ifndef LSBATCH_H
    #include "lsf/lsbatch.h"
    #include "lsf/lssched.h"
  #endif
}

// Definitions
static const int HANDLER_SHMEM_ID  = 103;
static const int HANDLER_PYTHON_ID = 104;

// Configuration
#define DEFAULT_NOTIFY_DIR   "/var/www/html/lsf/"
#define DEFAULT_POLICY_FILE  "/etc/castor/policies/scheduler.py"

// Throttling of error messages
#define PYERR_THROTTLING 60
#define PYINI_THROTTLING 60

// Customised pending reasons
#define PEND_HOST_CUNKNOWN    20003 // Host not listed in shared memory
#define PEND_HOST_CSTATE      20004 // Diskserver status incorrect
#define PEND_HOST_CNOTRFS     20005 // Diskserver is not in the list of RFS
#define PEND_HOST_CNOINTEREST 20006 // Diskserver not of interest e.g no space
#define PEND_HOST_PYERR       20007 // Embedded python interpreter error

#endif // SCHEDULER_CONSTANTS_HPP
