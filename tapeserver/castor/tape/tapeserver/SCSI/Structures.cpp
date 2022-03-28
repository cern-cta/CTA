/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "Structures.hpp"

#include <sstream>
#include <cstdio>

std::string castor::tape::SCSI::Structures::toString(const inquiryData_t& inq) {
  std::stringstream inqDump;
  inqDump << std::hex << std::showbase << std::nouppercase
          << "inq.perifDevType=" << (int) inq.perifDevType << std::endl
          << "inq.perifQualifyer=" << (int) inq.perifQualifyer << std::endl
          << "inq.RMB="            << (int) inq.RMB << std::endl
          << "inq.version="        << (int) inq.version << std::endl
          << "inq.respDataFmt="    << (int) inq.respDataFmt << std::endl
          << "inq.HiSup="          << (int) inq.HiSup << std::endl
          << "inq.normACA="        << (int) inq.normACA << std::endl
          << "inq.addLength="      << (int) inq.addLength << std::endl
          << "inq.protect="        << (int) inq.protect << std::endl
          << "inq.threePC="        << (int) inq.threePC << std::endl
          << "inq.TPGS="           << (int) inq.TPGS << std::endl
          << "inq.ACC="            << (int) inq.ACC << std::endl
          << "inq.SCCS="           << (int) inq.SCCS << std::endl         
          << "inq.addr16="         << (int) inq.addr16 << std::endl       
          << "inq.multiP="         << (int) inq.multiP << std::endl
          << "inq.VS1="            << (int) inq.VS1 << std::endl
          << "inq.encServ="        << (int) inq.encServ << std::endl
          << "inq.VS2="            << (int) inq.VS2 << std::endl
          << "inq.cmdQue="         << (int) inq.cmdQue << std::endl
          << "inq.sync="           << (int) inq.sync << std::endl
          << "inq.wbus16="         << (int) inq.wbus16  << std::endl 
          << "inq.T10Vendor="      << SCSI::Structures::toString(inq.T10Vendor) << std::endl
          << "inq.prodId="         << SCSI::Structures::toString(inq.prodId) << std::endl
          << "inq.prodRevLv="      << SCSI::Structures::toString(inq.prodRevLvl) << std::endl
          << "inq.vendorSpecific1="<< SCSI::Structures::toString(inq.vendorSpecific1)<< std::endl
          << "inq.IUS="            << (int) inq.IUS << std::endl
          << "inq.QAS="            << (int) inq.QAS << std::endl
          << "inq.clocking="       << (int) inq.clocking << std::endl;
  for (int i=0; i < 8; i++)
    inqDump << "inq.versionDescriptor[" << i << "]=" << SCSI::Structures::toU16(inq.versionDescriptor[i]) << std::endl;
  return inqDump.str();
}
