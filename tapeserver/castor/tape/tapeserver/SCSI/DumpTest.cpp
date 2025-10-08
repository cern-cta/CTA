/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

/**
 * Test main program. For development use.
 */

#include "../System/Wrapper.hpp"
#include "Device.hpp"
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
