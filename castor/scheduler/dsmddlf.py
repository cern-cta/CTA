#/******************************************************************************
# *                   dsmddlf.py
# *
# * This file is part of the Castor project.
# * See http://castor.web.cern.ch/castor
# *
# * Copyright (C) 2003  CERN
# * This program is free software; you can redistribute it and/or
# * modify it under the terms of the GNU General Public License
# * as published by the Free Software Foundation; either version 2
# * of the License, or (at your option) any later version.
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# *
# * @(#)$RCSfile: castor_tools.py,v $ $Revision: 1.9 $ $Release$ $Date: 2009/03/23 15:47:41 $ $Author: sponcec3 $
# *
# * declares all dlf messages used in the castor disk server manager daemon
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import dlf

# a little, useful enum type
def enum(*args):
    enums = dict(zip(args, range(len(args))))
    return type('Enum', (), enums)

# declarations of all messages constants
msgs = enum('SCHEDULEJOBCALLED', 'NBJOBSCALLED', 'BADMINCALLED',
            'BJOBSCALLED', 'JOBSETCALLED', 'BKILLCALLED',
            'GETQUEUINGJOBSCALLED', 'GETRUNNINGD2DSOURCECALLED',
            'D2DENDCALLED', 'JOBSTARTING', 'JOBALREADYSTARTED',
            'POSTPONEDFORSRCNOTREADY', 'JOBSTARTINGFAILED',
            'ACTIVITYCONTROLEXCEPTION', 'MGMTEXCEPTION',
            'NOQUEUERETRIEVED', 'SIGNALRECEIVED', 'UNEXPECTEDEXCEPTION',
            'POPULATING', 'FOUNDJOBALREADYRUNNING', 'JOBENDED',
            'INFORMJOBISOVERFAILED', 'INFORMJOBKILLEDFAILED',
            'RETRYJOB', 'INVALIDTIMEOUTOPTION')

# initialization of the messages
dlf.addmessages({msgs.SCHEDULEJOBCALLED : 'scheduleJob called',
                 msgs.NBJOBSCALLED : 'nbJobs called',
                 msgs.BADMINCALLED : 'badmin called',
                 msgs.BJOBSCALLED : 'bjobs called',
                 msgs.JOBSETCALLED : 'jobset called',
                 msgs.BKILLCALLED : 'bkill called',
                 msgs.GETQUEUINGJOBSCALLED : 'getQueuingJobs called',
                 msgs.GETRUNNINGD2DSOURCECALLED : 'dSource called',
                 msgs.D2DENDCALLED : 'd2dend called',
                 msgs.JOBSTARTING : 'Job starting',
                 msgs.JOBALREADYSTARTED : 'Job start canceled (already started on another host)',
                 msgs.POSTPONEDFORSRCNOTREADY : 'Start postponned until source is ready',
                 msgs.JOBSTARTINGFAILED : 'Job starting failed',
                 msgs.ACTIVITYCONTROLEXCEPTION : 'Caught exception in ActivityControl thread',
                 msgs.MGMTEXCEPTION : 'Caught exception in Management thread',
                 msgs.NOQUEUERETRIEVED : 'No queue could be retrieved',
                 msgs.SIGNALRECEIVED : 'Received signal',
                 msgs.UNEXPECTEDEXCEPTION : 'Caught unexpected exception, exiting',
                 msgs.POPULATING : 'populating running scripts from system',
                 msgs.FOUNDJOBALREADYRUNNING : 'Found job already running',
                 msgs.JOBENDED : 'job ended',
                 msgs.INFORMJOBISOVERFAILED : 'Informing scheduler that d2d transfer is over failed',
                 msgs.INFORMJOBKILLEDFAILED : 'Informing scheduler that jobs were killed by signals failed',
                 msgs.RETRYJOB : 'Retrying job',
                 msgs.INVALIDTIMEOUTOPTION : 'Invalid JobManager/PendingTimeouts option, ignoring entry'})
