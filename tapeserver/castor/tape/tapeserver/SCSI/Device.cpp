/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <cstdlib>

#include "common/exception/Errnum.hpp"
#include "Device.hpp"

using namespace castor::tape;

/**
 * Fill up the array that the device list is with all the system's
 * SCSI devices information.
 *
 * (all code using templates must be in the header file)
 */
SCSI::DeviceVector::DeviceVector(System::virtualWrapper& sysWrapper) : m_sysWrapper(sysWrapper) {
  std::string sysDevsPath = "/sys/bus/scsi/devices";
  cta::utils::Regex ifFirstCharIsDigit("^[[:digit:]]");
  std::vector<std::string> checkResult;
  DIR* dirp;
  cta::exception::Errnum::throwOnNull(
      dirp = m_sysWrapper.opendir(sysDevsPath.c_str()),
      "Error opening sysfs scsi devs");
  while (struct dirent * dent = m_sysWrapper.readdir(dirp)) {
    std::string dn(dent->d_name);
    if ("." == dn || ".." == dn) continue;
    checkResult = ifFirstCharIsDigit.exec(dn);
    if (0 == checkResult.size()) continue; /* we only use digital names like 6:0:3:0 */
    std::string fullpath = sysDevsPath + "/" + std::string(dent->d_name);
    /* We expect only symbolic links in this directory, */
    char rp[PATH_MAX];
    cta::exception::Errnum::throwOnNull(
        m_sysWrapper.realpath(fullpath.c_str(), rp),
        "Could not find realpath for " + fullpath);
    this->push_back(getDeviceInfo(rp));
  }
  sysWrapper.closedir(dirp);
}

SCSI::DeviceInfo & SCSI::DeviceVector::findBySymlink(const std::string& path) {
  struct stat sbuff;
  cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.stat(path.c_str(), &sbuff),
      std::string("Could not stat path: ")+path);
  for(std::vector<DeviceInfo>::iterator i = begin(); i != end(); ++i) {
    if (i->nst == sbuff || i->st == sbuff) {
      return *i;
    }
  }
  throw SCSI::DeviceVector::NotFound(
      std::string("Could not find tape device pointed to by ") + path);
}

std::string SCSI::DeviceVector::readfile(const std::string& path) {
  int fd;
  cta::exception::Errnum::throwOnMinusOne(
      fd = m_sysWrapper.open(path.c_str(), 0),
      std::string("Could not open file ") + path);
  char buf[readfileBlockSize];
  std::string ret;
  while (ssize_t sread = m_sysWrapper.read(fd, buf, readfileBlockSize)) {
    cta::exception::Errnum::throwOnMinusOne(sread,
      std::string("Could not read from open file ") + path);
    ret.append(buf, sread);
  }
  cta::exception::Errnum::throwOnNonZero(
      m_sysWrapper.close(fd),
      std::string("Error closing file ") + path);
  return ret;
}

SCSI::DeviceInfo::DeviceFile SCSI::DeviceVector::readDeviceFile(const std::string& path) {
  DeviceInfo::DeviceFile ret;
  std::string file = readfile(path);
  if (!::sscanf(file.c_str(), "%u:%u\n", &ret.major, &ret.minor))
    throw cta::exception::Exception(std::string("Could not parse file: ") + path);
  return ret;
}

SCSI::DeviceInfo::DeviceFile SCSI::DeviceVector::statDeviceFile(const std::string& path) {
  struct stat sbuf;
  cta::exception::Errnum::throwOnNonZero(
     m_sysWrapper.stat(path.c_str(), &sbuf),
     std::string("Could not stat file ") + path);
  if (!S_ISCHR(sbuf.st_mode))
    throw cta::exception::Exception("Device file " + path + " is not a character device");
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
    cta::exception::Errnum::throwOnNull(
        dirp = m_sysWrapper.opendir(devinfo.sysfs_entry.c_str()),
        std::string("Error opening tape device directory ") +
            devinfo.sysfs_entry + tapeDir+" or "+devinfo.sysfs_entry);
    /* we are not on SLC6 */
    scsiPrefix = "^scsi_tape:";
    tapeDir ="";
  }

  cta::utils::Regex st_re((scsiPrefix+"(st[[:digit:]]+)$").c_str());
  cta::utils::Regex nst_re((scsiPrefix+"(nst[[:digit:]]+)$").c_str());

  while (struct dirent * dent = m_sysWrapper.readdir(dirp)) {
    std::vector<std::string> res;
    /* Check if it's the st information */
    res = st_re.exec(dent->d_name);
    if (res.size()) {
      if (!devinfo.st_dev.size()) {
        devinfo.st_dev = std::string("/dev/") + res[1];
      } else
        throw cta::exception::Exception("Matched st device several times!");
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
        throw cta::exception::Exception(err.str());
      }
    }
    /* Check if it's the nst information */
    res = nst_re.exec(dent->d_name);
    if (res.size()) {
      if (!devinfo.nst_dev.size()) {
        devinfo.nst_dev = std::string("/dev/") + res[1];
      } else
        throw cta::exception::Exception("Matched nst device several times!");
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
        throw cta::exception::Exception(err.str());
      }
    }
  }
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
      throw cta::exception::Exception(std::string("Could not parse file: ") + ret.sysfs_entry + "/type");
  }
  /* Get vendor (trimmed of trailing newline, not of spaces) */
  {
    buf = readfile(ret.sysfs_entry + "/vendor");
    size_t endpos  = buf.find_first_of("\n ");
    ret.vendor = buf.substr(0, endpos);
  }
  /* Get model (trimmed of trailing newline, not of spaces) */
  {
    buf = readfile(ret.sysfs_entry + "/model");
    size_t endpos  = buf.find_first_of("\n ");
    ret.product = buf.substr(0, endpos);
  }
  /* Get revision (trimmed of trailing newline, not of spaces) */
  {
    buf = readfile(ret.sysfs_entry + "/rev");
    size_t endpos  = buf.find_first_of("\n ");
    ret.productRevisionLevel = buf.substr(0, endpos);
  }
  /* Get name of sg device */
  {
    char rl[PATH_MAX];
    std::string lp = ret.sysfs_entry + "/generic";
    ssize_t rlSize;
    cta::exception::Errnum::throwOnMinusOne(
        rlSize = m_sysWrapper.readlink(lp.c_str(), rl, sizeof (rl) - 1),
        std::string("Could not read link ") + lp);
    rl[rlSize] = '\0';
    std::string gl(rl);
    size_t pos = gl.find_last_of("/");
    if (pos == std::string::npos)
      throw cta::exception::Exception(std::string("Could not find last / in link: ") + gl +
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
    throw cta::exception::Exception(err.str());
  }
  /* Handle more if we have a tape device */
  if (Types::tape == ret.type)
    getTapeInfo(ret);
  return ret;
}
