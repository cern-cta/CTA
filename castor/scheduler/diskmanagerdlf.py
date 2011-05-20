#/******************************************************************************
# *                   diskmanagerdlf.py
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

'''Declaration of all log messages of the disk server manager daemon'''

import dlf

# declarations of all messages constants
msgs = dlf.enum('INVOKINGSCHEDULETRANSFER', 'INVOKINGSUMMARIZETRANSFERS',
                'INVOKINGLISTTRANSFERS', 'INVOKINGTRANSFERSET', 'INVOKINGKILLTRANSFERS',
                'INVOKINGGETQUEUEINGTRANSFERS', 'INVOKINGGETRUNNINGD2DSOURCE',
                'INVOKINGD2DEND', 'TRANSFERSTARTING', 'TRANSFERALREADYSTARTED',
                'POSTPONEDFORSRCNOTREADY', 'TRANSFERSTARTINGFAILED',
                'ACTIVITYCONTROLEXCEPTION', 'MGMTEXCEPTION',
                'NOQUEUERETRIEVED', 'SIGNALRECEIVED', 'UNEXPECTEDEXCEPTION',
                'POPULATING', 'FOUNDTRANSFERALREADYRUNNING', 'TRANSFERENDED',
                'INFORMTRANSFERISOVERFAILED', 'INFORMTRANSFERKILLEDFAILED',
                'RETRYTRANSFER', 'INVALIDTIMEOUTOPTION', 'ANYTRANSFERFROMSCHED',
                'INVOKINGTRANSFERALREADYSTARTED', 'SYNCRUNTRANSFERFAILED',
                'DISKMANAGERDSTARTED', 'DISKMANAGERDSTOPPED', 'INVOKINGRETRYD2DDEST',
                'TRANSFERSTARTTIMEOUT', 'ACTIVITYCHECKDROPCONN', 'ACTIVITYCHECKEXCEPTION')

# initialization of the messages
dlf.addmessages({msgs.INVOKINGSCHEDULETRANSFER : 'Invoking scheduleTransfer called',
                 msgs.INVOKINGSUMMARIZETRANSFERS : 'Invoking summarizeTransfers called',
                 msgs.INVOKINGLISTTRANSFERS : 'Invoking listTransfers called',
                 msgs.INVOKINGTRANSFERSET : 'Invoking transferset called',
                 msgs.INVOKINGKILLTRANSFERS : 'Invoking killtransfers called',
                 msgs.INVOKINGGETQUEUEINGTRANSFERS : 'Invoking getQueueingTransfers called',
                 msgs.INVOKINGGETRUNNINGD2DSOURCE : 'Invoking getRunningD2dSource called',
                 msgs.INVOKINGD2DEND : 'Invoking d2dend called',
                 msgs.TRANSFERSTARTING : 'Transfer starting',
                 msgs.TRANSFERALREADYSTARTED : 'Transfer start canceled (already started on another host)',
                 msgs.POSTPONEDFORSRCNOTREADY : 'Start postponed until source is ready',
                 msgs.TRANSFERSTARTINGFAILED : 'Failed to start transfer',
                 msgs.ACTIVITYCONTROLEXCEPTION : 'Caught exception in ActivityControl thread',
                 msgs.MGMTEXCEPTION : 'Caught exception in Management thread',
                 msgs.NOQUEUERETRIEVED : 'No queue could be retrieved',
                 msgs.SIGNALRECEIVED : 'Received signal',
                 msgs.UNEXPECTEDEXCEPTION : 'Caught unexpected exception, exiting',
                 msgs.POPULATING : 'Populating running scripts from system',
                 msgs.FOUNDTRANSFERALREADYRUNNING : 'Found transfer already running',
                 msgs.TRANSFERENDED : 'Transfer ended',
                 msgs.INFORMTRANSFERISOVERFAILED : 'Failed to inform scheduler that a d2d transfer is over',
                 msgs.INFORMTRANSFERKILLEDFAILED : 'Failed to inform scheduler that transfers were killed by signals',
                 msgs.RETRYTRANSFER : 'Retrying transfer',
                 msgs.INVALIDTIMEOUTOPTION : 'Invalid TransferManager/PendingTimeouts option, ignoring entry',
                 msgs.ANYTRANSFERFROMSCHED : 'Invoking anyTransfersFromScheduler called',
                 msgs.INVOKINGTRANSFERALREADYSTARTED : 'Invoking transferAlreadyStarted called',
                 msgs.SYNCRUNTRANSFERFAILED : 'Exception caught when trying to synchronize running transfers with the database. Giving up',
                 msgs.DISKMANAGERDSTARTED : 'DiskManager Daemon started',
                 msgs.DISKMANAGERDSTOPPED : 'DiskManager Daemon stopped',
                 msgs.INVOKINGRETRYD2DDEST : 'Invoking retryD2dDest called',
                 msgs.TRANSFERSTARTTIMEOUT : 'Timeout when trying to start/cancel transfer. Putting it back to the queue',
                 msgs.ACTIVITYCHECKDROPCONN : 'Detected stuck ActivityControl thread. Killed connections to transfermanagerd',
                 msgs.ACTIVITYCHECKEXCEPTION : 'Caught exception in ActivityControlChecker thread. Giving up for this round'})

