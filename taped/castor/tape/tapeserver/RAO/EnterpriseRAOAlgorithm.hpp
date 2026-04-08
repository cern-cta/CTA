/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "EnterpriseRAOAlgorithmFactory.hpp"
#include "RAOAlgorithm.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor::tape::tapeserver::rao {

class EnterpriseRAOAlgorithmFactory;

/**
 * This class represents an EnterpriseRAOAlgorithm.
 */
class EnterpriseRAOAlgorithm : public RAOAlgorithm {
public:
  friend EnterpriseRAOAlgorithmFactory;

  ~EnterpriseRAOAlgorithm() final = default;

  /**
   * Asks the Enteprise drive to perform a RAO query in order to get the RAO of the
   * files represented by the jobs passed in parameter
   * @param jobs the jobs representing the files we want to perform the RAO on
   * @return the vector of the indexes of the jobs passed in parameters rearranged by the RAO query
   */
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs) override;

  std::string getName() const override;

private:
  /**
   * Constructs an EnterpriseRAOAlgorithm
   * @param drive the drive in order to call its RAO via its DriveInterface
   * @param maxFilesSupported the maximum number of files supported by the drive to perform the RAO
   */
  EnterpriseRAOAlgorithm(drive::DriveInterface* drive, const uint64_t maxFilesSupported)
      : m_drive(drive),
        m_maxFilesSupported(maxFilesSupported) {}

  //Interface to the drive
  castor::tape::tapeserver::drive::DriveInterface* m_drive;
  //Maximum number of files supported by the drive to perform the RAO
  uint64_t m_maxFilesSupported;

  const uint32_t c_blockSize = 262144;
};

}  // namespace castor::tape::tapeserver::rao
