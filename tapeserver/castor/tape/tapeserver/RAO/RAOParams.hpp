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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "tapeserver/castor/tape/tapeserver/RAO/RAOOptions.hpp"

namespace castor::tape::tapeserver::rao {

/**
  * This class contains the configuration of the CTA RAO Algorithm
  */
class RAOParams {
public:
  /**
    * RAO algorithm types implemented by CTA
    */
  enum RAOAlgorithmType {
    linear,
    random,
    sltf    //!< Short Locate Time First
  };

  /**
   * Default constructor
   */
  RAOParams() = default;
  /**
   * Constructor
   *
   * @param useRAO if set to true, the RAO will be enabled. If false, not enabled.
   * @param raoAlgorithmName the RAO algorithm that will be executed when called
   * @param raoAlgorithmOptions the options that could be passed to the RAO algorithm
   * @param vid the vid of the tape that is currently mounted for retrieval
   */
  RAOParams(bool useRAO, std::string_view raoAlgorithmName, std::string_view raoAlgorithmOptions, std::string_view vid) :
    m_useRAO(useRAO), m_raoAlgorithmName(raoAlgorithmName), m_raoAlgorithmOptions(raoAlgorithmOptions), m_vid(vid) { }
  /**
    * Returns true if RAO has to be used, false otherwise
    */
  bool useRAO() const { return m_useRAO; }
  /**
    * Returns the RAO algorithm name of this RAO data instance
    */
  const std::string& getRAOAlgorithmName() const { return m_raoAlgorithmName; }
  /**
    * Returns the RAO algorithm options of this RAO data instance
    */
  RAOOptions& getRAOAlgorithmOptions() { return m_raoAlgorithmOptions; }
  /**
    * Disable RAO of this configuration
    */
  void disableRAO() { m_useRAO = false; }
  /**
    * Returns RAOAlgorithmType object corresopnding to this configration's raoAlgorithmName
    * @return the RAOAlgorithmType object corresopnding to this configration raoAlgorithmName
    * @throws cta::exception::Exception in case the algorithm name does not match any RAOAlgorithmType
    */
  RAOAlgorithmType getAlgorithmType() const;
  /**
    * Returns the CTA RAO algorithm names that can be used
    */
  std::string getCTARAOAlgorithmNameAvailable() const;
  /**
    * Returns the vid of the tape that is mounted for retrieval
    */
  std::string getMountedVid() const;

private:
  bool m_useRAO = false;
  std::string m_raoAlgorithmName;
  RAOOptions m_raoAlgorithmOptions;
  std::string m_vid;

  /**
   * Static map in order to match the string representing an algorithm name and its enum type
   */
  static const std::map<std::string, RAOAlgorithmType> c_raoAlgoStringTypeMap;
};

} // namespace castor::tape::tapeserver::rao
