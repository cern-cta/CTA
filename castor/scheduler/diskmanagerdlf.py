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

import dlf

# declarations of all messages constants
msgs = dlf.enum('SCHEDULETRANSFERCALLED', 'SUMMARIZETRANSFERSCALLED',
                'LISTTRANSFERSCALLED', 'TRANSFERSETCALLED', 'KILLTRANSFERSCALLED',
                'GETQUEUEINGTRANSFERSCALLED', 'GETRUNNINGD2DSOURCECALLED',
                'D2DENDCALLED', 'TRANSFERSTARTING', 'TRANSFERALREADYSTARTED',
                'POSTPONEDFORSRCNOTREADY', 'TRANSFERSTARTINGFAILED',
                'ACTIVITYCONTROLEXCEPTION', 'MGMTEXCEPTION',
                'NOQUEUERETRIEVED', 'SIGNALRECEIVED', 'UNEXPECTEDEXCEPTION',
                'POPULATING', 'FOUNDTRANSFERALREADYRUNNING', 'TRANSFERENDED',
                'INFORMTRANSFERISOVERFAILED', 'INFORMTRANSFERKILLEDFAILED',
                'RETRYTRANSFER', 'INVALIDTIMEOUTOPTION', 'DSREFRESHFAILED',
                'ANYTRANSFERFROMSCHED', 'TRANSFERALREADYSTARTEDCALLED')

# initialization of the messages
dlf.addmessages({msgs.SCHEDULETRANSFERCALLED : 'scheduleTransfer called',
                 msgs.SUMMARIZETRANSFERSCALLED : 'summarizeTransfers called',
                 msgs.LISTTRANSFERSCALLED : 'listTransfers called',
                 msgs.TRANSFERSETCALLED : 'transferset called',
                 msgs.KILLTRANSFERSCALLED : 'killtransfers called',
                 msgs.GETQUEUEINGTRANSFERSCALLED : 'getQueueingTransfers called',
                 msgs.GETRUNNINGD2DSOURCECALLED : 'getRunningD2dSource called',
                 msgs.D2DENDCALLED : 'd2dend called',
                 msgs.TRANSFERSTARTING : 'Transfer starting',
                 msgs.TRANSFERALREADYSTARTED : 'Transfer start canceled (already started on another host)',
                 msgs.POSTPONEDFORSRCNOTREADY : 'Start postponned until source is ready',
                 msgs.TRANSFERSTARTINGFAILED : 'Transfer starting failed',
                 msgs.ACTIVITYCONTROLEXCEPTION : 'Caught exception in ActivityControl thread',
                 msgs.MGMTEXCEPTION : 'Caught exception in Management thread',
                 msgs.NOQUEUERETRIEVED : 'No queue could be retrieved',
                 msgs.SIGNALRECEIVED : 'Received signal',
                 msgs.UNEXPECTEDEXCEPTION : 'Caught unexpected exception, exiting',
                 msgs.POPULATING : 'populating running scripts from system',
                 msgs.FOUNDTRANSFERALREADYRUNNING : 'Found transfer already running',
                 msgs.TRANSFERENDED : 'transfer ended',
                 msgs.INFORMTRANSFERISOVERFAILED : 'Informing scheduler that d2d transfer is over failed',
                 msgs.INFORMTRANSFERKILLEDFAILED : 'Informing scheduler that transfers were killed by signals failed',
                 msgs.RETRYTRANSFER : 'Retrying transfer',
                 msgs.INVALIDTIMEOUTOPTION : 'Invalid JobManager/PendingTimeouts option, ignoring entry',
                 msgs.DSREFRESHFAILED : 'failed to refresh list of diskservers, kept old list',
                 msgs.ANYTRANSFERFROMSCHED : 'anyTransfersFromScheduler called',
                 msgs.TRANSFERALREADYSTARTEDCALLED : 'transferAlreadyStarted called'})
