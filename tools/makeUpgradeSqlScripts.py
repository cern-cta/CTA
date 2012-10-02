#!/usr/bin/python
#******************************************************************************
#                      makeUpgradeSqlScripts.py
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#******************************************************************************

'''This script automatically creates a set of upgrade scripts for the different
databases used in a CASTOR setup. These scripts are basically empty, only changing
the current release number. They should be amended by hand if any change needs
to be performed'''

# Modules
import getopt
import sys, os

# usage function
def usage(exitCode):
  '''prints usage'''
  print 'Usage : ' + sys.argv[0] + ' [-h|--help] [-o|--oldversion <oldversion>] [-n|--newversion <newversion>] [-t|--transparent] [component [...]]'
  print '  give no component to create all scripts'
  sys.exit(exitCode)

# first parse the options
verbose = False
forcedOldVersion = None
forcedNewVersion = None
transparent = False
try:
  options, arguments = getopt.getopt(sys.argv[1:], 'hvo:n:t', ['help', 'verbose', 'oldversion', 'newversion'])
except Exception, parsingException:
  print parsingException
  usage(1)
for f, v in options:
  if f == '-h' or f == '--help':
    usage(0)
  elif f == '-v' or f == '--verbose':
    verbose = True
  elif f == '-o' or f == '--oldversion':
    forcedOldVersion = v
  elif f == '-n' or f == '--newversion':
    forcedNewVersion = v
  elif f == '-t' or f == '--transparent':
    transparent = True
  else:
    print "unknown option : " + f
    usage(1)

# All arguments are supposed to be components
# if none, this means all
allcomponents = ['cns', 'dlf', 'mon', 'stager', 'cupv', 'vdqm', 'vmgr']
if not arguments:
    components = set(allcomponents)
else:
    components = set()
    for arg in arguments:
        if arg.lower() not in allcomponents:
            print 'Unknown component ' + arg
            print 'valid components are ' + ', '.join(allcomponents)
            usage(1)
        components.add(arg.lower())

# check that we are in a reasonnable place
curdir = os.getcwd()
if not curdir.endswith('upgrades'):
    print 'It does not seem that this script was called from the upgrades directory of a CASTOR checkout'
    print 'This means that automatic version detection will probably fail'
    answer = raw_input('Do you still want to proceed [y/N] ? ')
    if answer.lower() not in ['y', 'yes']:
        sys.exit(1)

# version handling
class Version:
    '''A Castor version object able to parse versions and build a tag out of it'''
    def __init__(self, value):
        '''constructor, able to parse string versions'''
        if value.startswith('v'):
            value = value[1:]
        value = value.replace('.',' ')
        value = value.replace('_',' ')
        value = value.replace('-',' ')
        self._version = [int(digit) for digit in value.split()]

    def __str__(self):
        '''prints the version as a string'''
        return '.'.join([str(digit) for digit in self._version[0:3]]) + '-' + str(self._version[3])

    def __cmp__(self, other):
        '''compares two versions'''
        if self._version < other._version:
            return -1
        elif self._version == other._version:
            return 0
        else:
            return 1

    def inc(self):
        '''Increment the version by one'''
        self._version[3] += 1
        
    def tag(self):
        '''return the tag corresponding to a given version'''
        return '_'.join([str(digit) for digit in self._version])

    def next(self):
        '''return a version object for the next version'''
        n = Version(str(self))
        n.inc()
        return n

# method creating the header of the file
def writeHeader(fd, prevVersion, newVersion, component, schemaName):
    '''Writes the header of an upgrade SQL file'''
    fd.write('/******************************************************************************\n')
    fd.write(' *                 %s_%s_to_%s.sql\n' % (component, prevVersion, newVersion))
    fd.write(''' *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
''')
    fd.write(' * This script upgrades a CASTOR v%s %s database to v%s\n' % (prevVersion, schemaName, newVersion))
    fd.write(' *\n')
    fd.write(' * @author Castor Dev team, castor-dev@cern.ch\n')
    fd.write(' *****************************************************************************/\n\n')

# method writing the checks at the top of the upgrade script
def writeChecks(fd, schemaName, schemaVersion, prevVersion, newVersion):
    '''Writes the checks at the top of an upgrade SQL file'''
    releaseType = 'NON TRANSPARENT'
    if transparent: releaseType = 'TRANSPARENT'
    fd.write('''/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  -- If we have encountered an error rollback any previously non committed
  -- operations. This prevents the UPDATE of the UpgradeLog from committing
  -- inconsistent data to the database.
  ROLLBACK;
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '%s'
     AND release = '%s'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = '%s'
     AND release LIKE '%s%%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the %s before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('%s', '%s', '%s');
COMMIT;\n\n''' % (schemaVersion.tag(), newVersion.tag(), schemaName, prevVersion.tag(),
                  schemaName, schemaVersion.tag(), newVersion.tag(), releaseType))

# method writing the code for standard job management, that is stopping all jobs and scheduling restart in 15mn
def writeStandardJobMgmt(fd):
    '''Writes the code for standard job management, that is stopping all jobs and scheduling restart in 15m'''
    fd.write('''/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name, force=>TRUE);
    END IF;
    -- Schedule the start date of the job to 15 minutes from now. This
    -- basically pauses the job for 15 minutes so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 15/1440);
  END LOOP;
END;
/\n\n''')
            
# method writing some placeHolder comments for changes
def writePlaceHoldersForChanges(fd):
    '''Writes some placeHolder comments for changes'''
    fd.write('''/* Schema changes go here */
/**************************/


/* Update and revalidation of PL-SQL code */
/******************************************/\n\n\n''')

# method revalidating all PL/SQL code
def writePLSQLRevalidation(fd):
    '''Writes revalidation of all PL/SQL code'''
    fd.write('''/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PACKAGE BODY' THEN a.object_type := 'PACKAGE'; END IF;
    BEGIN
      EXECUTE IMMEDIATE 'ALTER ' ||a.object_type||' '||a.object_name||' COMPILE';
    EXCEPTION WHEN OTHERS THEN
      -- ignore, so that we continue compiling the other invalid items
      NULL;
    END;
  END LOOP;
END;
/\n\n''')

# method for writing the trailer of the upgrade script
def writeTrailer(fd, newVersion):
    '''Writes trailer of the upgrade script'''
    fd.write('''/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '%s';
COMMIT;\n''' % newVersion.tag())

# method trying to guess version numbers for a given component
def extractVersions(component):
    '''method trying to guess version numbers for a given component'''
    # list exsiting upgrade scripts
    updateScripts = [script for script in os.listdir(os.getcwd()) \
                     if script.startswith(component) and script.endswith('.sql')]
    if not forcedOldVersion:
        # find a previous upgrade script and deduce the latest version
        latestVersion = Version('0.0.0-0')
        latestScript = ''
        for script in updateScripts:
            new = script[:-3].split('_')[3]
            newVersion = Version(new)
            if latestVersion < newVersion:
                latestVersion = newVersion
                latestScript = script
        if not latestScript:
            latestVersion = Version(raw_input('latest version for %s (2.1.x-y): ' % component))
    else:
        latestVersion = Version(forcedOldVersion)
        latestScript = ''
        for script in updateScripts:
            new = script[:-3].split('_')[3]
            newVersion = Version(new)
            if latestVersion == newVersion:
                latestScript = script
    # find out the DB schema version
    if latestScript:
      for l in open(latestScript).readlines():
        if l.strip().startswith('WHERE schemaVersion = '):
          schemaVersion = Version(l.split("'")[1])
          break
    else:
      schemaVersion = Version(raw_input('SchemaVersion for %s  (2.1.x-y): ' % component))
    # find out the new version
    if not forcedNewVersion:
      newVersion = latestVersion.next()
    else:
      newVersion = Version(forcedNewVersion)
    # return all versions
    return schemaVersion, latestVersion, newVersion

# main method going through the creation of all upgrade scripts
for gcomponent in components:
    gschemaName = gcomponent.upper()
    gschemaVersion, gprevVersion, gnewVersion = extractVersions(gcomponent)
    gfd = open('%s_%s_to_%s.sql' % (gcomponent, gprevVersion, gnewVersion), 'w')
    writeHeader(gfd, gprevVersion, gnewVersion, gcomponent, gschemaName)
    writeChecks(gfd, gschemaName, gschemaVersion, gprevVersion, gnewVersion)
    if gcomponent == 'stager':
        writeStandardJobMgmt(gfd)
    answer = raw_input('Any changes to the schema or PL/SQL code for the %s script [y/N] ? ' % gcomponent)
    if answer.lower() in ['y', 'yes']:
        writePlaceHoldersForChanges(gfd)
    writePLSQLRevalidation(gfd)
    writeTrailer(gfd, gnewVersion)
    gfd.close()
    print 'Sucessfully generated %s_%s_to_%s.sql' % (gcomponent, gprevVersion, gnewVersion)
