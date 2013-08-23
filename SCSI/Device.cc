// ----------------------------------------------------------------------
// File: SCSI/Device.cc
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <cstdlib>
#include <scsi/sg.h>

#include "Device.hh"

/**
 * Fill up the array that the device list is with all the system's
 * SCSI devices information.
 * 
 * (all code using templates must be in the header file)
 */
SCSI::DeviceVector::DeviceVector(Tape::System::virtualWrapper& sysWrapper) : m_sysWrapper(sysWrapper) {
  std::string sysDevsPath = "/sys/bus/scsi/devices";
  Tape::Utils::regex ifFirstCharIsDigit("^[[:digit:]]");
  std::vector<std::string> checkResult;
  DIR* dirp = m_sysWrapper.opendir(sysDevsPath.c_str());
  if (!dirp) throw Tape::Exceptions::Errnum("Error opening sysfs scsi devs");
  while (struct dirent * dent = m_sysWrapper.readdir(dirp)) {
    std::string dn(dent->d_name);
    if ("." == dn || ".." == dn) continue;
    checkResult = ifFirstCharIsDigit.exec(dn);
    if (0 == checkResult.size()) continue; /* we only use digital names like 6:0:3:0 */
    std::string fullpath = sysDevsPath + "/" + std::string(dent->d_name);
    /* We expect only symbolic links in this directory, */
    char rp[PATH_MAX];
    if (NULL == m_sysWrapper.realpath(fullpath.c_str(), rp))
      throw Tape::Exceptions::Errnum("Could not find realpath for " + fullpath);
    this->push_back(getDeviceInfo(rp));
  }
  sysWrapper.closedir(dirp);
}

std::string SCSI::DeviceVector::readfile(std::string path) {
  int fd = m_sysWrapper.open(path.c_str(), 0);
  if (-1 == fd) {
    throw Tape::Exceptions::Errnum("Could not open file " + path);
  }
  char buf[readfileBlockSize];
  std::string ret;
  while (ssize_t sread = m_sysWrapper.read(fd, buf, readfileBlockSize)) {
    if (-1 == sread)
      throw Tape::Exceptions::Errnum("Could not read from open file " + path);
    ret.append(buf, sread);
  }
  if (m_sysWrapper.close(fd))
    throw Tape::Exceptions::Errnum("Error closing file " + path);
  return ret;
}

SCSI::DeviceInfo::DeviceFile SCSI::DeviceVector::readDeviceFile(std::string path) {
  DeviceInfo::DeviceFile ret;
  std::string file = readfile(path);
  if (!::sscanf(file.c_str(), "%d:%d\n", &ret.major, &ret.minor))
    throw Tape::Exception(std::string("Could not parse file: ") + path);
  return ret;
}

SCSI::DeviceInfo::DeviceFile SCSI::DeviceVector::statDeviceFile(std::string path) {
  struct stat sbuf;
  if (m_sysWrapper.stat(path.c_str(), &sbuf))
    throw Tape::Exceptions::Errnum("Could not stat file " + path);
  if (!S_ISCHR(sbuf.st_mode))
    throw Tape::Exception("Device file " + path + " is not a character device");
  DeviceInfo::DeviceFile ret;
  ret.major = major(sbuf.st_rdev);
  ret.minor = minor(sbuf.st_rdev);
  return ret;
}

/**
 * Part factored out of getDeviceInfo: get the tape specifics
 * from sysfs.
 * @param devinfo
 */
void SCSI::DeviceVector::getTapeInfo(DeviceInfo & devinfo) {
  /* Find the st and nst devices for this SCSI device */
  Tape::Utils::regex *st_re;
  Tape::Utils::regex *nst_re;
  DIR * dirp; 
  /* SLC6 default tapeDir and scsiPrefix */
  std::string tapeDir="/scsi_tape"; /* The name of the tape dir inside of
                                     * devinfo.sysfs_entry. /scsi_tape/ on SLC6.
                                     */
  std::string scsiPrefix="^";        /* The prefix for the name for the tape 
                                      * device name for the regexp.
                                      * scsi_tape: on SLC5.
                                      */ 
  /* we try to open /scsi_tape first for SLC6 */
  dirp = m_sysWrapper.opendir((devinfo.sysfs_entry+tapeDir).c_str());
  
  if (!dirp) {
    /* here we open sysfs_entry for SLC5 */
    dirp = m_sysWrapper.opendir(devinfo.sysfs_entry.c_str());
    if (!dirp) throw Tape::Exceptions::Errnum(
      std::string("Error opening tape device directory ") +
      devinfo.sysfs_entry + tapeDir+" or "+devinfo.sysfs_entry);
    else {
      /* we are not on SLC6 */
      scsiPrefix="^scsi_tape:"; 
      tapeDir ="";
    }
  } 
  
  st_re = new Tape::Utils::regex((scsiPrefix+"(st[[:digit:]]+)$").c_str());
  nst_re = new Tape::Utils::regex((scsiPrefix+"(nst[[:digit:]]+)$").c_str());
  
  while (struct dirent * dent = m_sysWrapper.readdir(dirp)) {
    std::vector<std::string> res;
    /* Check if it's the st information */
    res = st_re->exec(dent->d_name);
    if (res.size()) {
      if (!devinfo.st_dev.size()) {
        devinfo.st_dev = std::string("/dev/") + res[1];     
      } else
        throw Tape::Exception("Matched st device several times!");
      /* Read the major and major number */
      devinfo.st = readDeviceFile(devinfo.sysfs_entry + tapeDir+ "/"
          + std::string(dent->d_name) + "/dev");
      /* Check the actual device file */
      DeviceInfo::DeviceFile realFile = statDeviceFile(devinfo.st_dev);
      if (devinfo.st != realFile) {
        std::stringstream err;
        err << "Mismatch between sysfs info and actual device file: "
            << devinfo.sysfs_entry + "/" + dent->d_name << " indicates "
            << devinfo.st.major << ":" << devinfo.st.minor
            << " while " << devinfo.st_dev << " is: "
            << realFile.major << ":" << realFile.minor;
        throw Tape::Exception(err.str());
      }
    }
    /* Check if it's the nst information */
    res = nst_re->exec(dent->d_name);
    if (res.size()) {
      if (!devinfo.nst_dev.size()) {
        devinfo.nst_dev = std::string("/dev/") + res[1];
      } else
        throw Tape::Exception("Matched nst device several times!");
      /* Read the major and major number */
      devinfo.nst = readDeviceFile(devinfo.sysfs_entry + tapeDir + "/"
          + std::string(dent->d_name) + "/dev");
      /* Check the actual device file */
      DeviceInfo::DeviceFile realFile = statDeviceFile(devinfo.nst_dev);
      if (devinfo.nst != realFile) {
        std::stringstream err;
        err << "Mismatch between sysfs info and actual device file: "
            << devinfo.sysfs_entry + "/" + dent->d_name << " indicates "
            << devinfo.nst.major << ":" << devinfo.nst.minor
            << " while " << devinfo.st_dev << " is: "
            << realFile.major << ":" << realFile.minor;
        throw Tape::Exception(err.str());
      }
    }
  }
  delete st_re;
  delete nst_re;
  m_sysWrapper.closedir(dirp);
}

/**
 * Extract information from sysfs about a SCSI device.
 * @param path Path to the directory with information about 
 * @return 
 */
SCSI::DeviceInfo SCSI::DeviceVector::getDeviceInfo(const char * path) {
  DeviceInfo ret;
  ret.sysfs_entry = path;
  std::string buf;
  /* Get device type */
  {
    buf = readfile(ret.sysfs_entry + "/type");
    if (!sscanf(buf.c_str(), "%d", &ret.type))
      throw Tape::Exception(std::string("Could not parse file: ") + ret.sysfs_entry + "/type");
  }
  /* Get name of sg device */
  {
    char rl[PATH_MAX];
    std::string lp = ret.sysfs_entry + "/generic";
    if (-1 == m_sysWrapper.readlink(lp.c_str(), rl, sizeof (rl)))
      throw Tape::Exceptions::Errnum("Could not read link " + lp);
    std::string gl(rl);
    size_t pos = gl.find_last_of("/");
    if (pos == std::string::npos)
      throw Tape::Exception(std::string("Could not find last / in link: ") + gl +
        " read from " + ret.sysfs_entry + "/generic");
    ret.sg_dev = std::string("/dev/") + gl.substr(pos + 1);
  }
  /* Get the major and minor number of the device file */
  ret.sg = readDeviceFile(ret.sysfs_entry + "/generic/dev");
  /* Check that we have an agreement with the actual device file */
  DeviceInfo::DeviceFile realFile = statDeviceFile(ret.sg_dev);
  if (ret.sg != realFile) {
    std::stringstream err;
    err << "Mismatch between sysfs info and actual device file: "
        << ret.sysfs_entry + "/generic/dev" << " indicates "
        << ret.sg.major << ":" << ret.sg.minor
        << " while " << ret.sg_dev << " is: "
        << realFile.major << ":" << realFile.minor;
    throw Tape::Exception(err.str());
  }
  /* Handle more if we have a tape device */
  if (Types::tape == ret.type)
    getTapeInfo(ret);
  return ret;
}
