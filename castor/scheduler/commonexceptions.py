#!/usr/bin/python
#/******************************************************************************
# *                   commonexceptions.py
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
# * defines custom exceptions of the scheduler framework
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""Defines custom exceptions of the scheduler frameworks"""

class TransferCanceled(Exception):
    """Exception sent by the transfermanager when a transfer that
       would like to start was already started somewhere else or
       was actually canceled in the mean time"""
    def __init__(self, message):
        """constructor"""
        super(TransferCanceled, self).__init__(message)

class SourceNotStarted(Exception):
    """Exception sent by the transfermanager when a d2ddst transfer
       that would like to start should be put on hold until the
       corresponding d2dsrc transfer starts"""
    def __init__(self):
        """constructor"""
        super(SourceNotStarted, self).__init__('')

