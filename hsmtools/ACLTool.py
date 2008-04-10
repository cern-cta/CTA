#!/usr/bin/python
#/******************************************************************************
# *                      ACLTool.py
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
#/******************************************************************************
import os
import sys

from optparse import OptionParser

try:
  import cx_Oracle
except:
  print '''Fatal: could not load module cx_Oracle.
  Make sure it is installed and $PYTHONPATH includes the directory where cx_Oracle.so resides and that
  your ORACLE environment is setup properly.\n'''
  sys.exit(2)



# Return a connection to the database
def initializeDB ():

  # Connect to the stager database. 
  # The connection string is username/password@nameofDB.  
  try:
    stagerDb = "XXXXXXX/YYYYYYYY@ZZZZZZZZ"
    connection = cx_Oracle.Connection(stagerDb)  
  except:
    print "Exception encountered connecting to DataBase "

  return connection  


# Close a database connection
def disconnectDB (connection):
  connection.close ()


# Define the parameters/options accepted from the command line
# Set the description of the program and the help
#
# Return the parser

def initOpts():
  parser = OptionParser(description= 'Add or drop privileges for a given set of parameters', prog= 'BWadminTool.py', usage='%prog -a|-d [-s svcclass| -u uuid |-g guid |-r regType] [-h --help]  ')
  parser.add_option("-a", "--add", action="store_true", dest="addition",
                 help="To add privileges to the svcClass, uuid, guid and for an specific request.")
  parser.add_option("-d", "--delete", action="store_true", dest="deletion",
                 help="To add privileges to the svcClass, uuid, guid and for an specific request.")
  parser.add_option("-s", "--svcClass",  dest="svcclass",
                  help="the svcClass where you want to get or remove thee privileges")
  parser.add_option("-u", "--uuid", dest="uuid",
                  help="the request Type where you want to get or remove the privileges")
  parser.add_option("-g", "--guid", type="int", dest="guid",
                  help="the guid where you want to get or remove the privileges")
  parser.add_option("-r", "--reqType", type="int", dest="reqType",
                  help="the request Type where you want to get or remove the privileges")

  return parser

# Get & check the input parameters, parse them
# Initialize the query to execute depending on the operation specified 
#
# return the sql query

def getOpts(options):

  sql='''
        CREATE OR REPLACE PROCEDURE adminBW(svcClass in  varchar2, euid in number, egid in number, reqType in number) AS
           p castorBW.Privilege;
         BEGIN
           p.svcClass := svcClass;
           p.euid := euid;
           p.egid := egid;
           p.reqType := reqType;
  '''
  
  if (options.addition):
    if (not options.deletion):
      print "add"
      sql =sql+ " castorBW.addPrivilege(p); END;"
    else:
      print "Please specify ONLY one of the options -a (to add) or -d (to remove). Please use --help for more information"
      os._exit(0)
  else:
    if(options.deletion):
      print "delete"
      sql =sql+ " castorBW.removePrivilege(p); END;"
    else:   
      print "You should specify -a (to add) or -d (to remove). Please use --help for more information"
      os._exit(0)

  return sql


      
#Check if the svcclass specified exists in the DB
#
# Return True if the svcclass is defined or exit the execution program

def checkSvcClass(cursor,svc):
  #print svc
  cursor.execute(" select * from svcclass where name=:1",[svc['svcclass']]);
  cursor.fetchall()

  if (cursor.rowcount > 0):
    return True
  else:
    print "That svcclass is not defined in this database"  
    os._exit(0)



#Check if the Request Type specified exists in the DB
#
# Return True if the reqType is defined or exit the execution program

def checkReqType(cursor, reqType):
  cursor.execute(" select * from type2Obj where type = :1",[reqType['reqType']]);
  cursor.fetchall()

  if (cursor.rowcount > 0):
    return True
  else:  
    print "This request Type is not defined in this database"
    os._exit(0)


#main
connection = initializeDB() 
cursor = cx_Oracle.Cursor(connection)

parser=initOpts()
(options,args)=parser.parse_args()
Param={'svcclass':options.svcclass,'uuid':options.uuid, 'guid': options.guid, 'reqType': options.reqType}
query= getOpts(options)


# Check the value of the parameters
if (options.svcclass):
   if ( not checkSvcClass(cursor,Param)):
     os._exit(0)

# Check the value of the parameters
if (options.reqType):
   if ( not checkReqType(cursor,Param)):
     os._exit(0)

#checkSvcClass(cursor,"default")
cursor.execute(query)
test= "begin adminBW(:svcclass,:uuid,:guid,:reqType); end;"
cursor.execute(test,Param)
cursor.execute("commit")
print "Changes commited on the DB"
cursor.close()
disconnectDB(connection)

