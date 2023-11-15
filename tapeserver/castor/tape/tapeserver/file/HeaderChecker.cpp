/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <sstream>
#include <string>

#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/OsmFileStructure.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace castor::tape::tapeFile {

void HeaderChecker::checkVOL1(const VOL1 &vol1, const std::string &volId)  {
  if (vol1.getVSN().compare(volId)) {
    std::ostringstream ex_str;
    ex_str << "[HeaderChecker::checkVOL1()] - VSN of tape (" << vol1.getVSN()
           << ") is not the one requested (" << volId << ")";
    throw TapeFormatError(ex_str.str());
  }
}

void HeaderChecker::checkOSM(const osm::LABEL &osmLabel, const std::string &volId) {
  if (osmLabel.name().compare(volId) != 0) {
    std::stringstream ex_str;
    ex_str << "[OsmReadSession::OsmReadSession()] - VSN of tape (" << osmLabel.name()
            << ") is not the one requested (" << volId << ")";
    throw TapeFormatError(ex_str.str());
  }
}

bool HeaderChecker::checkHeaderNumericalField(const std::string &headerField,
  const uint64_t value, const HeaderBase& base)  {
  uint64_t res = 0;
  std::stringstream field_converter;
  field_converter << headerField;
  switch (base) {
    case HeaderBase::octal:
      field_converter >> std::oct >> res;
      break;
    case HeaderBase::decimal:
      field_converter >> std::dec >> res;
      break;
    case HeaderBase::hexadecimal:
      field_converter >> std::hex >> res;
      break;
    default:
      throw cta::exception::InvalidArgument("Unrecognised base in HeaderChecker::checkHeaderNumericalField");
  }
  return value == res;
}

void HeaderChecker::checkHDR1(const HDR1 &hdr1,
  const cta::RetrieveJob &filetoRecall,
  const tape::tapeserver::daemon::VolumeInfo &volInfo)  {
  const std::string &volId = volInfo.vid;
  if (!checkHeaderNumericalField(hdr1.getFileId(),
    filetoRecall.retrieveRequest.archiveFileID, HeaderBase::hexadecimal)) {
    // the nsfileid stored in HDR1 as an hexadecimal string . The one in
    // filetoRecall is numeric
    std::ostringstream ex_str;
    ex_str << "[HeaderChecker::checkHDR1] - Invalid fileid detected: (0x)\""
        << hdr1.getFileId() << "\". Wanted: 0x" << std::hex
        << filetoRecall.retrieveRequest.archiveFileID << std::endl;
    throw TapeFormatError(ex_str.str());
  }

  // the following should never ever happen... but never say never...
  if (hdr1.getVSN().compare(volId)) {
    std::ostringstream ex_str;
    ex_str << "[HeaderChecker::checkHDR1] - Wrong volume ID info found in hdr1: "
        << hdr1.getVSN() << ". Wanted: " << volId;
    throw TapeFormatError(ex_str.str());
  }
}

void HeaderChecker::checkUHL1(const UHL1 &uhl1,
  const cta::RetrieveJob &fileToRecall)  {
  if (!checkHeaderNumericalField(uhl1.getfSeq(),
    fileToRecall.selectedTapeFile().fSeq, HeaderBase::decimal)) {
    std::ostringstream ex_str;
    ex_str << "[HeaderChecker::checkUHL1] - Invalid fseq detected in uhl1: \""
        << uhl1.getfSeq() << "\". Wanted: "
        << fileToRecall.selectedTapeFile().fSeq;
    throw TapeFormatError(ex_str.str());
  }
}

void HeaderChecker::checkUTL1(const UTL1 &utl1, const uint32_t fSeq)  {
  if (!checkHeaderNumericalField(utl1.getfSeq(), static_cast<uint64_t>(fSeq), HeaderBase::decimal)) {
    std::ostringstream ex_str;
    ex_str << "[HeaderChecker::checkUTL1] - Invalid fseq detected in utl1: \""
            << utl1.getfSeq() << "\". Wanted: " << fSeq;
    throw TapeFormatError(ex_str.str());
  }
}

std::string HeaderChecker::checkVolumeLabel(tapeserver::drive::DriveInterface &drive, LabelFormat labelFormat) {
  std::string volumeLabelVSN;
  auto vol1Label = [](tapeserver::drive::DriveInterface &drive, const std::string &expectedLblStandard) {
    tapeFile::VOL1 vol1;
    drive.readExactBlock(reinterpret_cast<void *>(&vol1), sizeof(vol1),
      "[HeaderChecker::checkVolumeLabel()] - Reading header VOL1");
    vol1.verify(expectedLblStandard.c_str());
    return vol1.getVSN();
  };

  auto osmLabel = [](tapeserver::drive::DriveInterface &drive) {
    tapeFile::osm::LABEL osmLabel;
    drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()),
    tapeFile::osm::LIMITS::MAXMRECSIZE,
    "[HeaderChecker::checkVolumeLabel()] - Reading OSM label - part 1");
    drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + tapeFile::osm::LIMITS::MAXMRECSIZE),
    tapeFile::osm::LIMITS::MAXMRECSIZE,
    "[HeaderChecker::checkVolumeLabel()] - Reading OSM label - part 2");
    osmLabel.decode();
    return osmLabel.name();
  };

  try {
    switch (labelFormat) {
      case LabelFormat::CTA:
        volumeLabelVSN = vol1Label(drive, "3");
        break;
      case LabelFormat::OSM:
        volumeLabelVSN = osmLabel(drive);
        break;
      case LabelFormat::Enstore:
        volumeLabelVSN = vol1Label(drive, "0");
        break;
      default: {
        cta::exception::Exception ex;
        ex.getMessage() << "In HeaderChecker::checkVolumeLabel(): unknown label format: ";
        ex.getMessage() << std::showbase << std::internal << std::setfill('0') << std::hex << std::setw(4)
                        << static_cast<unsigned int>(labelFormat);
        throw ex;
      }
    }
  } catch(cta::exception::Exception &ne) {
    std::ostringstream ex_str;
    ex_str << "Failed to check volume label: " << ne.getMessageValue();
    throw TapeFormatError(ex_str.str());
  }

  return volumeLabelVSN;
}

} // namespace castor::tape::tapeFile
