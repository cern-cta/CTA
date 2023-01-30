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

/**
 * Test main program. For development use.
 */

#include "tapeserver/castor/tape/tapeserver/system/Wrapper.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Device.hpp"
#include <iostream>

int main ()
{
  Tape::System::realWrapper sWrapper;
  SCSI::DeviceVector<Tape::System::realWrapper> dl(sWrapper);
  for(SCSI::DeviceVector<Tape::System::realWrapper>::iterator i = dl.begin();
          i != dl.end(); ++i) {
    SCSI::DeviceInfo & dev = (*i);
    std::cout << dev.sg_path << std::endl;
  }
}
