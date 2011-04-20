#/******************************************************************************
# *                   transfermanagerdlf.py
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
# * declares all dlf messages used in the CASTOR's transfer manager daemon
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''Declaration of all log messages of the transfer manager daemon'''

import dlf

# declarations of all messages constants
msgs = dlf.enum('ABORTEREXCEPTION', 'SYNCHROFAILED', 'SYNCHROEXCEPTION',
                'TRANSFERALREADYSTARTED', 'SOURCENOTREADY', 'D2DOVERINFORMFAILED',
                'TRANSFERCANCELEXCEPTION', 'WORKEREXCEPTION', 'FAILEDTRANSFER',
                'FAILINGTRANSFEREXCEPTION', 'TRANSFERSCHEDULED', 'TRANSFERSCHEDULEDEXCEPTION',
                'SCHEDD2DSRC', 'SCHEDD2DDEST', 'SCHEDTRANSFER',
                'SCHEDD2DSRCFAILED', 'SCHEDD2DDESTFAILED', 'SCHEDTRANSFERFAILED',
                'DISPATCHEXCEPTION', 'SUMMARIZETRANSFERSPERPOOLCALLED', 'SUMMARIZETRANSFERSPERHOSTCALLED',
                'LISTTRANSFERSCALLED', 'KILLTRANSFERSCALLED', 'KILLALLTRANSFERSCALLED',
                'KILLTRANSFERSINTERNALCALLED', 'KILLALLTRANSFERSINTERNALCALLED',
                'TRANSFERSKILLEDCALLED', 'TRANSFERSCANCELEDCALLED', 'TRANSFERSTARTINGCALLED',
                'GETQUEUEINGTRANSFERSCALLED', 'GETRUNNINGD2DSOURCETRANSFERSCALLED', 'D2DENDCALLED',
                'DRAINCALLED', 'SYNCHROKILLEDTRANSFER', 'FAILTRANSFEREXCEPTION',
                'NOQUEUERETRIEVED', 'SIGNALRECEIVED', 'UNEXPECTEDEXCEPTION',
                'COULDNOTCONTACTDS', 'SYNCDBWITHTM', 'SYNCNODISCREPANCY',
                'INFODSJOBSTARTED', 'INFODSJOBSTARTEDFAILED', 'DSREFRESHFAILED',
                'SYNCRUNCALLED', 'SYNCRUNEXCEPTION', 'RUNTRANSFERDISAPPEARED',
                'TRANSFERMANAGERDSTARTED', 'TRANSFERMANAGERDSTOPPED', 'NOD2DLEFTBEHIND',
                'D2DSYNCFAILED', 'SYNCHROENDEDTRANSFER', 'GETALLRUNNINGD2DSOURCETRANSFERSCALLED',
                'SYNCDBWITHD2DSRC', 'COULDNOTCONTACTTM', 'TRANSFERSTARTCONFIRMED')

# initialization of the messages
dlf.addmessages({msgs.ABORTEREXCEPTION : 'Caught exception in Aborter thread',
                 msgs.SYNCHROFAILED : 'Error caught while trying to synchronize DB transfers with scheduler transfers. Giving up for this round.',
                 msgs.SYNCHROEXCEPTION : 'Caught exception in Synchronizer thread',
                 msgs.TRANSFERALREADYSTARTED : 'Transfer had already started. Cancel start',
                 msgs.SOURCENOTREADY : 'Source is not ready yet',
                 msgs.D2DOVERINFORMFAILED : 'Failed to inform diskserver that a d2d copy is over',
                 msgs.TRANSFERCANCELEXCEPTION : 'Unexpected KeyError exception caught in transfersCanceled',
                 msgs.WORKEREXCEPTION : 'Exception caught in Worker thread',
                 msgs.FAILEDTRANSFER : 'Failed transfer',
                 msgs.FAILINGTRANSFEREXCEPTION : 'Exception caught while failing transfer',
                 msgs.TRANSFERSCHEDULED : 'Marking transfer as scheduled',
                 msgs.TRANSFERSCHEDULEDEXCEPTION : 'Exception caught while marking transfer scheduled',
                 msgs.SCHEDD2DSRC : 'Scheduling d2d source',
                 msgs.SCHEDD2DDEST : 'Scheduling d2d destination',
                 msgs.SCHEDTRANSFER : 'Scheduling standard transfer',
                 msgs.SCHEDD2DSRCFAILED : 'Failed to schedule d2d source',
                 msgs.SCHEDD2DDESTFAILED : 'Failed to schedule d2d destination',
                 msgs.SCHEDTRANSFERFAILED : 'Failed to schedule standard transfer',
                 msgs.DISPATCHEXCEPTION : 'Exception caught in Dispatcher thread',
                 msgs.SUMMARIZETRANSFERSPERPOOLCALLED : 'summarizeTransfersPerPool called',
                 msgs.SUMMARIZETRANSFERSPERHOSTCALLED : 'summarizeTransfersPerHost called',
                 msgs.LISTTRANSFERSCALLED : 'listTransfers called',
                 msgs.KILLALLTRANSFERSCALLED : 'killalltransfers called',
                 msgs.KILLTRANSFERSCALLED : 'killtransfers called',
                 msgs.KILLALLTRANSFERSINTERNALCALLED : 'killalltransfersinternal called',
                 msgs.KILLTRANSFERSINTERNALCALLED : 'killtransfersinternal called',
                 msgs.TRANSFERSKILLEDCALLED : 'transfersKilled called',
                 msgs.TRANSFERSCANCELEDCALLED : 'transfersCanceled called',
                 msgs.TRANSFERSTARTINGCALLED : 'transferStarting called',
                 msgs.GETQUEUEINGTRANSFERSCALLED : 'getQueueingTransfers called',
                 msgs.GETRUNNINGD2DSOURCETRANSFERSCALLED : 'getRunningD2dSourceTransfers called',
                 msgs.D2DENDCALLED : 'd2dend called',
                 msgs.DRAINCALLED : 'drain called',
                 msgs.SYNCHROKILLEDTRANSFER : 'Transfer killed by synchronization as it disappeared from the scheduling system',
                 msgs.FAILTRANSFEREXCEPTION : 'Exception caught while failing transfer',
                 msgs.NOQUEUERETRIEVED : 'No queue could be retrieved',
                 msgs.SIGNALRECEIVED : 'Received signal',
                 msgs.UNEXPECTEDEXCEPTION : 'Caught unexpected exception, exiting',
                 msgs.COULDNOTCONTACTDS : 'Could not contact diskserver',
                 msgs.SYNCDBWITHTM : 'Synchronizing stager DB with Transfer Manager',
                 msgs.SYNCNODISCREPANCY : 'No discrepancy during synchronization',
                 msgs.INFODSJOBSTARTED : "Informing diskserver that job started somewhere else",
                 msgs.INFODSJOBSTARTEDFAILED : "Failed to inform diskserver that job started elsewhere",
                 msgs.DSREFRESHFAILED : 'Failed to refresh list of diskservers, kept old list',
                 msgs.SYNCRUNCALLED : 'syncRunningTransfers called',
                 msgs.SYNCRUNEXCEPTION : 'Exception caught while synchronizing running transfers, giving up with synchronization',
                 msgs.RUNTRANSFERDISAPPEARED : 'Transfer was marked as failed in stager DB after it disappeared from the diskserver',
                 msgs.TRANSFERMANAGERDSTARTED : 'TransferManager Daemon started',
                 msgs.TRANSFERMANAGERDSTOPPED : 'TransferManager Daemon stopped',
                 msgs.NOD2DLEFTBEHIND : 'No disk to disk source source left behind',
                 msgs.D2DSYNCFAILED : 'Error caught while trying to get rid of disk to disk sources left behind. Giving up for this round.',
                 msgs.SYNCHROENDEDTRANSFER : 'Transfer ended by synchronization as the transfer disappeared from the DB',
                 msgs.GETALLRUNNINGD2DSOURCETRANSFERSCALLED : 'getAllRunningD2dSourceTransfers called',
                 msgs.SYNCDBWITHD2DSRC : 'Synchronizing stager DB with running d2d sources',
                 msgs.COULDNOTCONTACTTM : 'Could not contact transfer manager',
                 msgs.TRANSFERSTARTCONFIRMED : 'Transfer starting reconfirmed'})

