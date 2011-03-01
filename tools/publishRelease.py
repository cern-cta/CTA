#!/usr/bin/python
#******************************************************************************
#                      publishRelease.py
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

# Modules
import getopt
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib2
import xml.sax.handler

# Constants
TEAMCITY_SERVER       = "http://teamcity-dss:8111"
REST_URL              = "/guestAuth/app/rest/builds/?locator="
INTERNAL_RELEASE_AREA = "/afs/cern.ch/project/castor/www/DIST/intReleases/"

#------------------------------------------------------------------------------
# BuildsListXMLHandler
#------------------------------------------------------------------------------
class BuildsListXMLHandler(xml.sax.handler.ContentHandler):
    """
    Class to handle the response from the teamcity server REST build query. E.g
    <builds count="1">
      <build id="8603" number="54"
             status="SUCCESS"
             buildTypeId="bt31"
             href="/guestAuth/app/rest/builds/id:8603"
             webUrl="http://lxbrl2711:8111/viewLog.html?buildId=8603&.... />
    </builds>
    """

    def __init__(self):
        """ BuildsListXMLHandler constructor. """
        self._builds = []

    def startElement(self, name, attributes):
        """ Element processing. """

        # Ignore non <build> element tags.
        if name != "build":
            return

        # Convert the XML information into a dictionary structure and store
        # the result in the builds list.
        buildInfo = {}
        buildInfo['BuildId']     = attributes.get('id')
        buildInfo['BuildTypeId'] = attributes.get('buildTypeId')
        buildInfo['WebURL']      = attributes.get('webUrl')
        buildInfo['ArtifactURL'] = "/".join([
                TEAMCITY_SERVER + "/guestAuth/repository/downloadAll",
                attributes.get('buildTypeId'),
                attributes.get('id') + ":id",
                "artifacts.zip" ])
        self._builds.append(buildInfo)

    def getBuilds(self):
        """ Return the list of builds """
        return self._builds

#------------------------------------------------------------------------------
# RunCommand
#------------------------------------------------------------------------------
def runCommand(cmdline, useShell=False):
    """ A simple wrapper function for executing a command. """
    process = subprocess.Popen(cmdline, shell=useShell,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    output = process.communicate()[0]

    # If the command returns an error record it to the terminal and exit with
    # a non zero exit code.
    if process.returncode != 0:
        print "\n", output.strip()
        sys.exit(1)

    return output

#------------------------------------------------------------------------------
# Usage
#------------------------------------------------------------------------------
def usage(program_name, status):
    if status != 0:
        print >> sys.stderr, "Try `" + program_name + " --help for more information."
    else:
        print "Usage:", program_name, "[OPTIONS]..."
        print "  -t, --tag=v2.1.MAJOR-MINOR  The tag to be published."
        print "      --help                  Display this help and exit\n"
        print "Report bugs to <castor-support@cern.ch>"
    sys.exit(status)

#------------------------------------------------------------------------------
# Main
#------------------------------------------------------------------------------
def main(argv):

    # Parse command line arguments.
    releasedir = INTERNAL_RELEASE_AREA
    tag = "v2.1.MAJOR-MINOR"
    try:
        options, args = getopt.getopt(argv[1:], "tph", [ "tag=", "help" ])
        if len(args):
            usage(sys.argv[0], 1)
    except getopt.GetoptError, e:
        print >> sts.stderr, sys.argv[0] + ": " + str(e)
        usage(sys.argv[0], 1)

    for opt, arg in options:
        if opt in ("-h", "--help"):
            usage(sys.argv[0], 0)
        elif opt in ("-t", "--tag"):
            tag = str(arg)

    # Check that the tag has been supplied with the correct format.
    if not re.match("v(\d+).(\d+).(\d+)-(\d+)", tag):
        print >> sys.stderr, "[ERROR] Invalid tag:", tag, "- expected format: v2.1.MAJOR-MINOR e.g. v2.1.10-0"
        sys.exit(1)

    # Check the release directory to see if the release already exists.
    releasedir = os.path.join(releasedir, tag[1:])
    if os.path.exists(releasedir):
        print >> sys.stderr, "[ERROR] Release directory:", releasedir, "already exists!"
        sys.exit(1)

    # Create a temporary directory to construct the release artifacts.
    tmpdir = tempfile.mkdtemp()
    os.chdir(tmpdir)

    # Query the Teamcity webserver using the REST API to see if a build exists
    # for the tag supplied by the user.2
    # Note: A production/internal release must:
    #  - Not be a personal build
    #  - Have been tagged appropriately eg. v2_1_10_1
    #  - Have been successful
    #  - Must be a pinned build
    url = TEAMCITY_SERVER + REST_URL
    url += ",".join([ "pinned:true",
                      "tags:" + tag,
                      "personal:false",
                      "status:SUCCESS" ])

    print "[STEP 1/9] - Querying TeamCity server:", TEAMCITY_SERVER, "for release information"

    xml_handler = BuildsListXMLHandler()
    f = urllib2.urlopen(url)
    xml.sax.parseString(f.read(), xml_handler)
    f.close()

    # Check the results from the query to TeamCity, we should have one and only
    # one result!
    if len(xml_handler.getBuilds()) == 0:
        print "[STEP 1/9] - No builds found for tag:", tag
        sys.exit(1)
    elif len(xml_handler.getBuilds()) > 1:
        print "[STEP 1/9] - Too many build configurations found for tag:", tag
        sys.exit(1)

    # Log the information about the release.
    buildInfo = xml_handler.getBuilds()[0]
    print "[STEP 1/9] - Found 1 build configuration:"
    print "[STEP 1/9] -   BuildTypeId: ", buildInfo['BuildTypeId']
    print "[STEP 1/9] -   BuildId:     ", buildInfo['BuildId']
    print "[STEP 1/9] -   WebURL:      ", buildInfo['WebURL']
    print "[STEP 1/9] -   ArtifactURL: ", buildInfo['ArtifactURL']

    # Download the artifacts zip file.
    print "[STEP 2/9] - Downloading artifacts:"
    print "[STEP 2/9] -  ", buildInfo['ArtifactURL'], "=>", tmpdir

    # Note: We use wget as opposed to the urllib library for convenience!
    zipfile = os.path.join(tmpdir, "artifacts.zip")
    runCommand([ "/usr/bin/wget", buildInfo['ArtifactURL'],
                 "--no-check-certificate", "-O", zipfile ])

    # Decompress the downloaded artifacts.
    print "[STEP 3/9] - Decompressing:", zipfile
    runCommand([ "/usr/bin/unzip", zipfile, "-d", tmpdir ])

    # List the contents of the directory after decompression. This in theory
    # should just be the dist-version-arch directories!
    os.unlink(zipfile)
    for name in sorted(os.listdir(tmpdir)):
        print "[STEP 3/9] -  ", name

    # Move the artifacts to the correct directories.
    print "[STEP 4/9] - Creating artifact layout:"
    for name in sorted(os.listdir(tmpdir)):
        filepath = os.path.join(tmpdir, name)
        if not os.path.isdir(filepath):
            continue

        # Extract the linux distribution, version and architecture from the
        # directory name.
        (dist, version, arch) = name.split("-")
        dist = dist.upper()[0:2] + version

        # Create the new destination directory.
        dstdir = os.path.join(tmpdir, dist)
        if not os.path.exists(dstdir):
            os.mkdir(dstdir)
        dstdir = os.path.join(dstdir, arch)

        # Copy the srcdir artifacts to their final destination.
        print "[STEP 4/9] -   Moving:", filepath, "=>", dstdir
        shutil.move(filepath, dstdir)

    # Extract the source tarball from one of the source rpms. The first step to
    # achieve this is to find a source RPM.
    print "[STEP 5/9] - Extracting source tarball from source RPM:"
    output = runCommand([ "/usr/bin/find", tmpdir, "-type", "f",
                          "-name", "*.src.rpm" ])

    # Record the source RPM name being used.
    srcrpm = output.split("\n")[0]
    print "[STEP 5/9] -   Source RPM:    ", srcrpm

    # Using a combination of the rpm2cpio and cpio command extract the contents
    # of the source RPM. For CASTOR this should result in two new files the
    # source tarball and the spec file.
    runCommand(" ".join([ "/usr/bin/rpm2cpio", srcrpm,
                          "| /bin/cpio -i -d" ]), useShell=True)

    # Cleanup the temporary directory removing any file which is not a
    # directory or a tarball.
    tarfile = None
    for name in os.listdir(tmpdir):
        filepath = os.path.join(tmpdir, name)
        if not os.path.isfile(filepath):
            continue
        if not name.endswith(".tar.gz"):
            os.unlink(filepath)
        else:
            tarfile = filepath

    print "[STEP 5/9] -   Source Tarball:", tarfile

    # Create a directory for the contents of the tarball.
    dstdir = os.path.join(tmpdir, "extracted")
    os.mkdir(dstdir)

    # Extract the tarball contents. Note: We strip the first directory from
    # the output for convenience!
    runCommand([ "/bin/tar", "-zxf", tarfile, "-C", dstdir,
                 "--strip-components", "1" ])

    print "[STEP 6/9] - Publishing release support files:"

    # Create a list of files to be copied out of the extracted tarball
    # contents.
    files = { 'Release Notes':    ("",           []),
              'Upgrade Scripts':  ("dbupgrades", []),
              'Creation Scripts': ("dbcreation", []) }
    files['Release Notes'][1].append(
        os.path.join(tmpdir, "extracted/ReleaseNotes"))

    # Loop over all files in the upgrade directory.
    srcdir = os.path.join(tmpdir, "extracted/upgrades")
    for name in os.listdir(srcdir):
        filepath = os.path.join(srcdir, name)
        if not os.path.isfile(filepath):
            continue
        if not name.endswith(".sql"):
            continue
        tokens = name[0:-4].split("_")
        if len(tokens) < 4:
            continue

        # If the old or new version matches the tag we are operating on then
        # we have found either an upgrade or downgrade script which should
        # be copied to the dbupgrades directory.
        if tokens[1] == tag[1:] or tokens[3] == tag[1:]:
            files['Upgrade Scripts'][1].append(filepath)

    # Collect all the creation scripts.
    files['Creation Scripts'][1].append(
        os.path.join(tmpdir, "extracted/castor/db/drop_oracle_schema.sql"))
    files['Creation Scripts'][1].append(
        os.path.join(tmpdir, "extracted/castor/db/grant_oracle_user.sql"))

    output = runCommand([ "/usr/bin/find", os.path.join(tmpdir, "extracted"),
                          "-type", "f", "-name", "*_oracle_create.sql" ])
    for line in output.split("\n"):
        if not line:
            continue
        files['Creation Scripts'][1].append(line)

    # Perform the actual copy of the files to their final destination.
    for type, value in files.iteritems():
        print "[STEP 6/9] -  ", type
        for filepath in value[1]:
            print "[STEP 6/9] -    ", filepath.replace(tmpdir, "")
            dstdir = os.path.join(tmpdir, value[0])
            if not os.path.exists(dstdir):
                os.makedirs(dstdir)
            shutil.move(filepath, dstdir)

    # Create the tarball containing the testsuite.
    tarfile = os.path.join(tmpdir, "testsuite.tar.gz")
    print "[STEP 7/9] - Creating Test Suite tarball:"
    print "[STEP 7/9] -  ", tarfile

    # Copy the testsuite to a dedicated directory.
    srcdir = os.path.join(tmpdir, "extracted/test/testsuite")
    dstdir = os.path.join(tmpdir, "testsuite")
    shutil.copytree(srcdir, dstdir)

    # Remove unnecessary files from the test suite.
    os.unlink("testsuite/buildTestCase.py");

    # Create the testsuite tarball.
    runCommand([ "/bin/tar", "-C", tmpdir, "-zcf", tarfile, "testsuite" ])
    shutil.rmtree(dstdir)

    # Publish the temporary directory to its final location.
    print "[STEP 8/9] - Publishing temporary directory:"
    print "[STEP 8/9] -   ", releasedir

    shutil.rmtree(os.path.join(tmpdir, "extracted"));
    shutil.copytree(tmpdir, releasedir)

    # Remove the temporary directory.
    print "[STEP 9/9] - Removing temporary directory:", tmpdir
    shutil.rmtree(tmpdir)

#------------------------------------------------------------------------------
# Bootstrap
#------------------------------------------------------------------------------
if __name__ == '__main__':
    main(sys.argv)
