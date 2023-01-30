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

#pragma once

#include "tapeserver/castor/tape/tapeserver/RAO/RAOAlgorithm.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "tapeserver/castor/tape/tapeserver/RAO/EnterpriseRAOAlgorithmFactory.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

class EnterpriseRAOAlgorithmFactory;
  
/**
 * This class represents an EnterpriseRAOAlgorithm. 
 */
class EnterpriseRAOAlgorithm : public RAOAlgorithm{
public:
  friend EnterpriseRAOAlgorithmFactory;
  
  virtual ~EnterpriseRAOAlgorithm();

  /**
   * Asks the Enteprise drive to perform a RAO query in order to get the RAO of the 
   * files represented by the jobs passed in parameter
   * @param jobs the jobs representing the files we want to perform the RAO on
   * @return the vector of the indexes of the jobs passed in parameters rearranged by the RAO query
   */
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>> & jobs) override;
  
  std::string getName() const override;
  
private:
  /**
   * Constructs an EnterpriseRAOAlgorithm
   * @param drive the drive in order to call its RAO via its DriveInterface
   * @param maxFilesSupported the maximum number of files supported by the drive to perform the RAO
   */
  EnterpriseRAOAlgorithm(castor::tape::tapeserver::drive::DriveInterface * drive, const uint64_t maxFilesSupported);
  
  //Interface to the drive
  castor::tape::tapeserver::drive::DriveInterface * m_drive;
  //Maximum number of files supported by the drive to perform the RAO
  uint64_t m_maxFilesSupported;
  
  const uint32_t c_blockSize = 262144;
};

}}}}
