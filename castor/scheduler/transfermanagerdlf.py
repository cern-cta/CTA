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
                'INFODSJOBSTARTED', 'INFODSJOBSTARTEDFAILED', 'DSREFRESHFAILED')

# initialization of the messages
dlf.addmessages({msgs.ABORTEREXCEPTION : 'Caught exception in Aborter thread',
                 msgs.SYNCHROFAILED : 'Error caught while trying to synchronize DB transfers with scheduler transfers. Giving up for this round.',
                 msgs.SYNCHROEXCEPTION : 'Caught exception in Synchronizer thread',
                 msgs.TRANSFERALREADYSTARTED : 'Transfer had already started. Cancel start',
                 msgs.SOURCENOTREADY : 'Source is not ready yet',
                 msgs.D2DOVERINFORMFAILED : 'Informing diskserver that d2d copy is over failed',
                 msgs.TRANSFERCANCELEXCEPTION : 'Unexpected KeyError exception caught in transfersCanceled',
                 msgs.WORKEREXCEPTION : 'Exception caught in Worker thread',
                 msgs.FAILEDTRANSFER : 'Failed transfer',
                 msgs.FAILINGTRANSFEREXCEPTION : 'Exception caught while failing transfer',
                 msgs.TRANSFERSCHEDULED : 'Marking transfers scheduled',
                 msgs.TRANSFERSCHEDULEDEXCEPTION : 'Exception caught while marking transfer scheduled',
                 msgs.SCHEDD2DSRC : 'Scheduling d2d source',
                 msgs.SCHEDD2DDEST : 'Scheduling d2d destination',
                 msgs.SCHEDTRANSFER : 'Scheduling standard transfer',
                 msgs.SCHEDD2DSRCFAILED : 'Scheduling d2d source failed',
                 msgs.SCHEDD2DDESTFAILED : 'Scheduling d2d destination failed',
                 msgs.SCHEDTRANSFERFAILED : 'Scheduling standard transfer failed',
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
                 msgs.INFODSJOBSTARTEDFAILED : "Informing diskserver that job started somewhere else failed",
                 msgs.DSREFRESHFAILED : 'failed to refresh list of diskservers, kept old list'})
