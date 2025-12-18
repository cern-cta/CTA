/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Position.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * This class holds the position informations about a file : its physical position (start and end) and
 * informations about the band and the landing zone of the beginning and the end of the file.
 */
class FilePositionInfos {
public:
  /**
   * Default constructor
   */
  FilePositionInfos() = default;
  /**
   * Destructor
   */
  virtual ~FilePositionInfos() = default;

  /**
   * Set the beginning of the file position
   * @param beginningPosition the beginning of the file position
   */
  void setBeginningPosition(const Position& beginningPosition) { m_beginningPosition = beginningPosition; }

  /**
   * Set the end of the file position
   * @param endPosition the end of the file position
   */
  void setEndPosition(const Position& endPosition) { m_endPosition = endPosition; }

  /**
   * Set the beginning of the file band
   * @param beginningBand the beginning of the file band
   */
  void setBeginningBand(uint8_t beginningBand) { m_beginningBand = beginningBand; }

  /**
   * Set the end of the file band
   * @param endBand the end of the file band
   */
  void setEndBand(uint8_t endBand) { m_endBand = endBand; }

  /**
   * Set the beginning of the file landing zone
   * @param startLandingZone
   */
  void setBeginningLandingZone(uint8_t beginningLandingZone) { m_beginningLandingZone = beginningLandingZone; }

  /**
   * Set the end of the file landing zone
   * @param endLandingZone
   */
  void setEndLandingZone(const uint8_t endLandingZone) { m_endLandingZone = endLandingZone; }

  /**
   * Get the physical position of the beginning of the file
   */
  Position getBeginningPosition() const { return m_beginningPosition; }

  /**
   * Get the physical position of the end of the file
   */
  Position getEndPosition() const { return m_endPosition; }

  /**
   * Get the beginning of the file band
   */
  uint8_t getBeginningBand() const { return m_beginningBand; }

  /**
   * Get the end of the file band
   */
  uint8_t getEndBand() const { return m_endBand; }

  /**
   * Get the beginning of the file landing zone
   */
  uint8_t getBeginningLandingZone() const { return m_beginningLandingZone; }

  /**
   * Get the end of the file landing zone
   */
  uint8_t getEndLandingZone() const { return m_endLandingZone; }

private:
  Position m_beginningPosition;
  Position m_endPosition;
  uint8_t m_beginningBand = 0;
  uint8_t m_endBand = 0;
  uint8_t m_beginningLandingZone = 0;
  uint8_t m_endLandingZone = 0;
};

}  // namespace castor::tape::tapeserver::rao
