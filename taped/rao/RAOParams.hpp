/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RAOOptions.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace cta::tape::rao {

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
    sltf  //!< Short Locate Time First
  };

  /// Construct empty algorithm settings.
  RAOParams() = default;

  /// Store the selected algorithm, its options and the mounted tape's VID.
  RAOParams(std::string_view raoAlgorithmName, std::string_view raoAlgorithmOptions, std::string_view vid)
      : m_raoAlgorithmName(raoAlgorithmName),
        m_raoAlgorithmOptions(raoAlgorithmOptions),
        m_vid(vid) {}

  /**
    * Returns the RAO algorithm name of this RAO data instance
    */
  const std::string& getRAOAlgorithmName() const { return m_raoAlgorithmName; }

  /**
    * Returns the RAO algorithm options of this RAO data instance
    */
  const RAOOptions& getRAOAlgorithmOptions() const { return m_raoAlgorithmOptions; }

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
  std::string m_raoAlgorithmName;
  RAOOptions m_raoAlgorithmOptions;
  std::string m_vid;

  /**
   * Static map in order to match the string representing an algorithm name and its enum type
   */
  static const std::map<std::string, RAOAlgorithmType> c_raoAlgoStringTypeMap;
};

}  // namespace cta::tape::rao
