#/******************************************************************************
# *                   modifydiskservers.py
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
# * ModifyDiskServer is part of the transfer manager of the CASTOR project.
# * This module allows to modify the properties of a set of diskservers
# * (e.g. state or diskPool) from a remote location such as the diskserver itself.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''This module allows to modify the properties of a set of diskservers
(e.g. state or diskPool) from a remote location such as the diskserver itself.'''

import castor_tools

def modifyDiskServers(dbconn, targets, state, mountPoints, diskPool, isRecursive):
    '''modifies the properties of a set of diskservers'''
    # consistency check
    if len(targets) == 0 and len(mountPoints) > 0:
        raise ValueError('targets are mandatory when mountPoints are given')
    # check target diskpool if any
    stcur = dbconn.cursor()
    if diskPool:
        sqlStatement = "SELECT id, name FROM DiskPool WHERE name = :diskPool"
        stcur.execute(sqlStatement, diskPool=diskPool)
        rows = stcur.fetchall()
        if not rows:
            raise ValueError('DiskPool %s does not exist. Giving up' % diskPool)
        diskPoolId = rows[0][0]
    # get list of target diskservers rfom the mix diskpool/diskserver targets
    unknownTargets, diskServerIds = castor_tools.parseAndCheckTargets(targets, stcur)
    # and complain in case of problem
    if not diskServerIds:
         raise ValueError('None of the provided diskpools/diskservers could be found. Giving up')
    # go for the update
    returnMsg = []
    mountPointIds = []
    if mountPoints == None:
        # update diskServers for status
        if state != None:
            sqlStatement = '''UPDATE DiskServer SET status = :status
                              WHERE id IN (''' + ', '.join([str(x) for x in diskServerIds]) + ')'
            stcur.execute(sqlStatement, status=state)
        # update fileSystems for diskPool
        if diskPool:
            sqlStatement = '''UPDATE FileSystem SET diskPool = :diskPoolId
                               WHERE diskServer IN (''' + ', '.join([str(x) for x in diskServerIds]) + ')'
            stcur.execute(sqlStatement, diskPoolId=diskPoolId)
        returnMsg.append('Diskserver(s) modified successfully')
        # in case we are running in recursive mode, list mountPoints
        if isRecursive:
            sqlStatement = 'SELECT id FROM FileSystem WHERE diskserver IN (' + \
                           ', '.join([str(x) for x in diskServerIds]) + ')'
            stcur.execute(sqlStatement)
            mountPointIds = [row[0] for row in stcur.fetchall()]
    else:
        # MountPoints were given by the user, check them
        sqlStatement = '''SELECT id, mountPoint FROM FileSystem
                           WHERE mountPoint = :mountPoint
                             AND (diskServer IN (''' + ', '.join([str(x) for x in diskServerIds]) + '))'
        existingMountPoints = set([])
        for mountPoint in mountPoints:
            stcur.execute(sqlStatement, mountPoint=mountPoint)
            row = stcur.fetchone()
            if row:
                mountPointIds.append(row[0])
                existingMountPoints.add(row[1])
        unknownMountPoints = set(m for m in mountPoints) - existingMountPoints
        # and set FileSystems' DiskPool if needed
        if diskPool:
            sqlStatement = 'UPDATE FileSystem SET diskPool = :diskPool WHERE id IN (' + \
                           ', '.join([str(x) for x in mountPointIds]) + ')'
            stcur.execute(sqlStatement, diskPool=diskPoolId)
            returnMsg.append('Filesystem(s)\' diskpools modified successfully')
    # Now status of mountpoints
    # they may have been given by the user or created by the handling of the recursive option
    if mountPointIds:
        # update mountPoints
        sqlStatement = 'UPDATE FileSystem SET status = :status WHERE id IN (' + \
                       ', '.join([str(x) for x in mountPointIds]) + ')'
        stcur.execute(sqlStatement, status=state)
        returnMsg.append('Filesystem(s)\' status modified successfully')
    # commit updates
    dbconn.commit()
    # mention unknown mountPoints
    if mountPoints and unknownMountPoints:
        returnMsg.append('WARNING : the following mountPoints could not be found : ' + \
                         ', '.join(unknownMountPoints))
    # mention unknown targets
    if unknownTargets:
        returnMsg.append('WARNING : the following diskpools/diskservers do not exist : ' + \
                         ', '.join(unknownTargets))
    # return collected messages. If any exception, this is forwarded to the caller
    return '\n'.join(returnMsg)
