/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <string>
#include "tapeserver/castor/tape/tapeserver/daemon/DataTransferConfig.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"
#include <memory>
#include <common/log/LogContext.hpp>
#include <vector>

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
  /**
   * This class contains the configuration of the CTA RAO Algorithm
   * It is initialized by the DataTransferSession and is filled from the configuration file of the tapeserver.
   */
  class RAOConfig{
  public:
    /**
     * This enum represent the RAO algorithm type implemented
     * by CTA
     */
    enum RAOAlgorithmType {
      linear,
      random,
      //Short Locate Time First
      sltf
    };

    RAOConfig();
    
    /**
     * Construct an RAO config
     * @param useRAO if set to true, the RAO will be enabled. If false, not enabled.
     * @param raoAlgorithmName the RAO algorithm that will be executed when called
     * @param raoAlgorithmOptions the options that could be passed to the RAO algorithm
     */
    RAOConfig(const bool useRAO, const std::string & raoAlgorithmName, const std::string raoAlgorithmOptions);
    
    /**
     * Returns true if RAO has to be used, false otherwise
     */
    bool useRAO() const;
    
    /**
     * Returns the RAO algorithm name of this configuration
     */
    std::string getRAOAlgorithmName() const;
    
    /**
     * Returns the RAO algorithm options of this configuration
     * @return 
     */
    std::string getRAOAlgorithmOptions() const;
    
    /**
     * Disable RAO from this configuration
     */
    void disableRAO();
    
    /**
     * Returns RAOAlgorithmType object corresopnding to this configration's raoAlgorithmName
     * @return the RAOAlgorithmType object corresopnding to this configration raoAlgorithmName
     * @throws cta::exception::Exception in case the algorithm name does not match any RAOAlgorithmType
     */
    RAOAlgorithmType getAlgorithmType() const;
    
    /**
     * Returns the CTA RAO algorithm name that can be used
     */
    std::string getCTARAOAlgorithmNameAvailable() const;
    
  private:
    bool m_useRAO = false;
    std::string m_raoAlgorithmName;
    std::string m_raoAlgorithmOptions;
    
    /**
     * Static map in order to match the string representing an algorithm name and its enum type
     */
    static const std::map<std::string, RAOAlgorithmType> c_raoAlgoStringTypeMap;
  };
  
  
}}}}