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

#pragma once

#include <string>

namespace cta {
class RetrieveJob;
}

namespace castor {
namespace tape {

namespace tapeserver {
namespace daemon {
class VolumeInfo;
}
}

namespace tapeFile {

class HDR1;
class UHL1;
class UTL1;
class VOL1;

class HeaderChecker {
 public:
  /**
    * Checks the hdr1
    * @param hdr1: the hdr1 header of the current file
    * @param filetoRecall: the Information structure of the current file
    * @param volId: the volume id of the tape in the drive
    */
  static void checkHDR1(const HDR1 &hdr1,
    const cta::RetrieveJob &filetoRecall,
    const tape::tapeserver::daemon::VolumeInfo &volInfo);

  /**
    * Checks the uhl1
    * @param uhl1: the uhl1 header of the current file
    * @param fileToRecall: the Information structure of the current file
    */
  static void checkUHL1(const UHL1 &uhl1, const cta::RetrieveJob &fileToRecall);

  /**
    * Checks the utl1
    * @param utl1: the utl1 trailer of the current file
    * @param fseq: the file sequence number of the current file
    */
  static void checkUTL1(const UTL1 &utl1, const uint32_t fseq);

  /**
    * Checks the vol1
    * @param vol1: the vol1 header of the current file
    * @param volId: the volume id of the tape in the drive
    */
  static void checkVOL1(const VOL1 &vol1, const std::string &volId);

 private:
  enum class HeaderBase {
    octal,
    decimal,
    hexadecimal
  };

  /**
    * Checks the field of a header comparing it with the numerical value provided
    * @param headerField: the string of the header that we need to check
    * @param value: the unsigned numerical value against which we check
    * @param is_field_hex: set to true if the value in the header is in hexadecimal and to false otherwise
    * @param is_field_oct: set to true if the value in the header is in octal and to false otherwise
    * @return true if the header field  matches the numerical value, false otherwise
    */
  static bool checkHeaderNumericalField(const std::string &headerField,
    const uint64_t value, const HeaderBase& base = HeaderBase::decimal);
};

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
