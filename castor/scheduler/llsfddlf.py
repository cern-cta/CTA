#/******************************************************************************
# *                   llsfddlf.py
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
# * declares all dlf messages used in the castor low latency scheduler facility daemon
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import dlf

# a little, useful enum type
def enum(*args):
    enums = dict(zip(args, range(len(args))))
    return type('Enum', (), enums)

# declarations of all messages constants
msgs = enum('ABORTEREXCEPTION', 'SYNCHROFAILED', 'SYNCHROEXCEPTION',
            'JOBALREADYSTARTED', 'SOURCENOTREADY', 'D2DOVERINFORMFAILED',
            'JOBCANCELEXCEPTION', 'WORKEREXCEPTION', 'FAILEDJOB',
            'FAILINGJOBEXCEPTION', 'JOBSCHEDULED', 'JOBSCHEDULEDEXCEPTION',
            'SCHEDD2DSRC', 'SCHEDD2DDEST', 'SCHEDJOB',
            'SCHEDD2DSRCFAILED', 'SCHEDD2DDESTFAILED', 'SCHEDJOBFAILED',
            'DISPATCHEXCEPTION', 'BQUEUESCALLED', 'BHOSTSCALLED',
            'BJOBSCALLED', 'BKILLCALLED',
            'BKILLINTERNALCALLED', 'JOBSKILLEDCALLED', 'JOBSCANCELEDCALLED',
            'JOBSTARTINGCALLED', 'GETQUEUINGJOBSCALLED', 'GETRUNNINGD2DSOURCEJOBSCALLED',
            'D2DENDCALLED', 'SYNCHROKILLEDJOB', 'FAILJOBEXCEPTION',
            'NOQUEUERETRIEVED', 'SIGNALRECEIVED', 'UNEXPECTEDEXCEPTION',
            'COULDNOTCONTACTDS')

# initialization of the messages
dlf.addmessages({msgs.ABORTEREXCEPTION : 'Caught exception in Aborter thread',
                 msgs.SYNCHROFAILED : 'Error caught while trying to synchronize DB jobs with scheduler jobs. Giving up for this round.',
                 msgs.SYNCHROEXCEPTION : 'Caught exception in Synchronizer thread',
                 msgs.JOBALREADYSTARTED : 'Job had already started. Cancel start',
                 msgs.SOURCENOTREADY : 'Source is not ready yet',
                 msgs.D2DOVERINFORMFAILED : 'Informing diskserver that d2d copy is over failed',
                 msgs.JOBCANCELEXCEPTION : 'Unexpected KeyError exception caught in jobsCanceled',
                 msgs.WORKEREXCEPTION : 'Exception caught in Worker thread',
                 msgs.FAILEDJOB : 'Failed job',
                 msgs.FAILINGJOBEXCEPTION : 'Exception caught while failing job',
                 msgs.JOBSCHEDULED : 'Marking jobs scheduled',
                 msgs.JOBSCHEDULEDEXCEPTION : 'Exception caught while marking job scheduled',
                 msgs.SCHEDD2DSRC : 'Scheduling d2d source',
                 msgs.SCHEDD2DDEST : 'Scheduling d2d destination',
                 msgs.SCHEDJOB : 'Scheduling standard job',
                 msgs.SCHEDD2DSRCFAILED : 'Scheduling d2d source failed',
                 msgs.SCHEDD2DDESTFAILED : 'Scheduling d2d destination failed',
                 msgs.SCHEDJOBFAILED : 'Scheduling standard job failed',
                 msgs.DISPATCHEXCEPTION : 'Exception caught in Dispatcher thread',
                 msgs.BQUEUESCALLED : 'bqueues called',
                 msgs.BHOSTSCALLED : 'bhosts called',
                 msgs.BJOBSCALLED : 'bjobs called',
                 msgs.BKILLCALLED : 'bkill called',
                 msgs.BKILLINTERNALCALLED : 'bkillinternal called',
                 msgs.JOBSKILLEDCALLED : 'jobsKilled called',
                 msgs.JOBSCANCELEDCALLED : 'jobsCanceled called',
                 msgs.JOBSTARTINGCALLED : 'jobStarting called',
                 msgs.GETQUEUINGJOBSCALLED : 'getQueuingJobs called',
                 msgs.GETRUNNINGD2DSOURCEJOBSCALLED : 'getRunningD2dSourceJobs called',
                 msgs.D2DENDCALLED : 'd2dend called',
                 msgs.SYNCHROKILLEDJOB : 'Job killed by synchronization as it disappeared from the scheduling system',
                 msgs.FAILJOBEXCEPTION : 'Exception caught while failing job',
                 msgs.NOQUEUERETRIEVED : 'No queue could be retrieved',
                 msgs.SIGNALRECEIVED : 'Received signal',
                 msgs.UNEXPECTEDEXCEPTION : 'Caught unexpected exception, exiting',
                 msgs.COULDNOTCONTACTDS : 'Could not contact diskserver'})                
