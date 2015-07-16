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
                'INVOKINGD2DEND', 'TRANSFERSTARTING', 'TRANSFERSTARTCANCELED',
                'POSTPONEDFORSRCNOTREADY', 'TRANSFERSTARTINGFAILED',
                'ACTIVITYCONTROLEXCEPTION', 'MGMTEXCEPTION',
                'NOQUEUERETRIEVED', 'SIGNALRECEIVED', 'UNEXPECTEDEXCEPTION',
                'POPULATING', 'FOUNDTRANSFERALREADYRUNNING', 'TRANSFERENDED',
                'TRANSFERENDEDFAILED', 'INFORMTRANSFERFAILED', 'INFORMTRANSFERFAILEDFAILED',
                'RETRYTRANSFER', 'INVALIDTIMEOUTOPTION', 'ANYTRANSFERFROMSCHED',
                'INVOKINGTRANSFERALREADYSTARTED', 'SYNCRUNTRANSFERFAILED',
                'DISKMANAGERDSTARTED', 'DISKMANAGERDSTOPPED', 'INVOKINGRETRYD2DDEST',
                'TRANSFERSTARTTIMEOUT', 'ACTIVITYCHECKDROPCONN', 'ACTIVITYCHECKEXCEPTION',
                'TRANSFERBACKTOQUEUEFAILED', 'REPORTEREXCEPTION', 'NOSCHEDULERINCONFIG',
                'SENDREPORTEXCEPTION', 'SENDREPORTFAILURE',
                'INVOKINGMACHINEDISABLED', 'INVOKINGFSDISABLED',
                'SCHEDFROMBACKFILL', 'PUTJOBINBACKFILL', 'SCHEDUSERJOBUNDERPRESSURE',
                'SCHEDPRIORITY', 'AVOIDBACKFILLSTARV', 'SCHEDUSERJOB',
                'MOVERHANDLEREXCEPTION', 'CLIENTSREPLIEREXCEPTION', 'CLIENTSLISTENEREXCEPTION',
                'MOVERSTARTING', 'MOVERSTARTFAILED', 'MOVERCALL',
                'TRANSFERTIMEDOUT', 'FAILTOQUERYXROOT',
                'SYNCRUNNINGTRANSFERS', 'INITQUEUES', 'EMPTYREPORT',
                'NOTAMOUNTPOINT', 'D2DRUNNINGSYNC', 'D2DRUNNINGSRCDROPPED')

# initialization of the messages
dlf.addmessages({msgs.INVOKINGSCHEDULETRANSFER : 'Invoking scheduleTransfer',
                 msgs.INVOKINGSUMMARIZETRANSFERS : 'Invoking summarizeTransfers',
                 msgs.INVOKINGLISTTRANSFERS : 'Invoking listTransfers',
                 msgs.INVOKINGTRANSFERSET : 'Invoking transferset',
                 msgs.INVOKINGKILLTRANSFERS : 'Invoking killtransfers',
                 msgs.INVOKINGGETQUEUEINGTRANSFERS : 'Invoking getQueueingTransfers',
                 msgs.INVOKINGGETRUNNINGD2DSOURCE : 'Invoking getRunningD2dSource',
                 msgs.INVOKINGD2DEND : 'Invoking d2dend',
                 msgs.TRANSFERSTARTING : 'Transfer starting',
                 msgs.TRANSFERSTARTCANCELED : 'Transfer start canceled',
                 msgs.POSTPONEDFORSRCNOTREADY : 'Start postponed until source is ready',
                 msgs.TRANSFERSTARTINGFAILED : 'Failed to start transfer',
                 msgs.ACTIVITYCONTROLEXCEPTION : 'Caught exception in ActivityControl thread',
                 msgs.MGMTEXCEPTION : 'Caught exception in Management thread',
                 msgs.NOQUEUERETRIEVED : 'No queue could be retrieved',
                 msgs.SIGNALRECEIVED : 'Received signal',
                 msgs.UNEXPECTEDEXCEPTION : 'Caught unexpected exception, exiting',
                 msgs.POPULATING : 'Populating running jobs from system',
                 msgs.FOUNDTRANSFERALREADYRUNNING : 'Found transfer already running',
                 msgs.TRANSFERENDED : 'Transfer ended',
                 msgs.TRANSFERENDEDFAILED : 'Failed to end the transfer',
                 msgs.INFORMTRANSFERFAILED : 'Informed scheduler that transfer failed',
                 msgs.INFORMTRANSFERFAILEDFAILED : 'Failed to inform scheduler that transfer failed',
                 msgs.RETRYTRANSFER : 'Retrying transfer',
                 msgs.INVALIDTIMEOUTOPTION : 'Invalid DiskManager/PendingTimeouts option, ignoring entry',
                 msgs.ANYTRANSFERFROMSCHED : 'Invoking anyTransfersFromScheduler',
                 msgs.INVOKINGTRANSFERALREADYSTARTED : 'Invoking transferAlreadyStarted',
                 msgs.SYNCRUNTRANSFERFAILED : 'Exception caught when trying to synchronize running transfers with the database. Giving up',
                 msgs.DISKMANAGERDSTARTED : 'DiskManager Daemon started',
                 msgs.DISKMANAGERDSTOPPED : 'DiskManager Daemon stopped',
                 msgs.INVOKINGRETRYD2DDEST : 'Invoking retryD2dDest',
                 msgs.TRANSFERSTARTTIMEOUT : 'Timeout when trying to start/cancel transfer. Putting it back to the queue',
                 msgs.ACTIVITYCHECKDROPCONN : 'Detected stuck ActivityControl thread. Killed connections to transfermanagerd',
                 msgs.ACTIVITYCHECKEXCEPTION : 'Caught exception in ActivityControlChecker thread. Giving up for this round',
                 msgs.TRANSFERBACKTOQUEUEFAILED : 'Failed to start transfer and got timeout when putting back to queue',
                 msgs.REPORTEREXCEPTION : 'Caught exception in Reporter thread. Giving up for this round',
                 msgs.NOSCHEDULERINCONFIG : 'No scheduler given in configfile, please fix',
                 msgs.SENDREPORTEXCEPTION : 'Caught exception when sending report to transfermanager, will try other transfermanagers',
                 msgs.SENDREPORTFAILURE : 'Could not send report to any transfermanager, giving up',
                 msgs.INVOKINGMACHINEDISABLED : 'Invoking machineDisabled',
                 msgs.INVOKINGFSDISABLED : 'Invoking FSDisabled',
                 msgs.SCHEDFROMBACKFILL : 'Not under pressure, scheduling work from the backfill queues',
                 msgs.PUTJOBINBACKFILL : 'Under pressure, putting job to d2dbackfill queue',
                 msgs.SCHEDUSERJOBUNDERPRESSURE : 'Under pressure, but still scheduling user job',
                 msgs.SCHEDPRIORITY : 'Scheduled job from priority queue',
                 msgs.AVOIDBACKFILLSTARV : 'Attempted to schedule one job from backfill according to MaxRegularJobsBeforeBackfill',
                 msgs.SCHEDUSERJOB : 'Scheduled job from regular queue',
                 msgs.MOVERHANDLEREXCEPTION : 'Caught exception in MoverHandler thread',
                 msgs.CLIENTSREPLIEREXCEPTION : 'Caught exception in ClientsReplier thread',
                 msgs.CLIENTSLISTENEREXCEPTION : 'Caught exception in ClientsListener thread',
                 msgs.MOVERSTARTING : 'Executing mover',
                 msgs.MOVERSTARTFAILED : 'Failed to execute mover',
                 msgs.MOVERCALL : 'Received call from mover',
                 msgs.TRANSFERTIMEDOUT : 'Transfer slot timed out',
                 msgs.FAILTOQUERYXROOT : 'Failed to query xrootd server',
                 msgs.SYNCRUNNINGTRANSFERS : 'Synchronizing running transfers with schedulers',
                 msgs.INITQUEUES : 'Initializing queues from schedulers',
                 msgs.EMPTYREPORT : 'Reporter found no mount points nor data pools to report, leaving diskserver offline',
                 msgs.NOTAMOUNTPOINT : 'Path is not a mount point, reporting 0 space',
                 msgs.D2DRUNNINGSYNC : 'running synchronization of d2dsrc transfers running',
                 msgs.D2DRUNNINGSRCDROPPED : 'd2dsrc transfer cleaned up as it is no more in the transfermanager'
                 })

