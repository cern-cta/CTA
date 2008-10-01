#/******************************************************************************
# *                   castor_tools.py
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
# * @(#)$RCSfile: castor_tools.py,v $ $Revision: 1.1 $ $Release$ $Date: 2008/10/01 12:56:54 $ $Author: sponcec3 $
# *
# * utility functions for castor tools written in python
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import os

def getOraStagerSvc():
  # find out the instance to use
  full_name = "DbCnvSvc";
  if os.environ.has_key('CASTOR_INSTANCE'):
      full_name = full_name + '_' + os.environ['CASTOR_INSTANCE']

  # go through the lines of ORASTAGERCONFIG
  user = "";
  passwd = "";
  dbname = "";
  for l in open ('/etc/castor/ORASTAGERCONFIG').readlines():
      try:
          instance, entry, value = l.split()
          if instance == full_name:
              if entry == 'user':
                  user = value
              elif entry == 'passwd':
                  passwd = value
              elif entry == 'dbName':
                  dbname = value
      except ValueError:
          # ignore line
          pass
  return user, passwd, dbname

# DiskCopy status
DiskCopyStatus = ["DISKCOPY_STAGED",
                  "DISKCOPY_WAITDISK2DISKCOPY",
                  "DISKCOPY_WAITTAPERECALL",
                  "DISKCOPY_DELETED",
                  "DISKCOPY_FAILED",
                  "DISKCOPY_WAITFS",
                  "DISKCOPY_STAGEOUT",
                  "DISKCOPY_INVALID",
                  "DISKCOPY_GCCANDIDATE",
                  "DISKCOPY_BEINGDELETED",
                  "DISKCOPY_CANBEMIGR",
                  "DISKCOPY_WAITFS_SCHEDULING"];

