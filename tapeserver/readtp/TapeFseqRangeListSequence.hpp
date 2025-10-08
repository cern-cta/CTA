/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "TapeFseqRange.hpp"
#include "TapeFseqRangeSequence.hpp"

#include <list>
#include <ostream>

namespace cta::tapeserver::readtp {

/**
 * A list of tape file sequence ranges.
 */
class TapeFseqRangeList: public  std::list<TapeFseqRange> {
}; // class TapeFseqRangeList

/**
 * Generates a sequence of tape file sequence numbers from a list of tape file
 * sequence ranges.
 */
class TapeFseqRangeListSequence {
public:

  /**
   * Constructor.
   *
   * Creates an empty sequence, in other word hasMore() will always return
   * false.
   */
  TapeFseqRangeListSequence() ;

  /**
   * Constructor
   *
   * @param list    The list of tape file sequence ranges from which the sequence of tape file sequence numbers is to be generated.
   */
  explicit TapeFseqRangeListSequence(const TapeFseqRangeList* const list);

  /**
   * Resets the sequence.
   *
   * @param list The list of tape file sequence ranges from which the sequence
   * of tape file sequence numbers is to be generated.
   */
  void reset(const TapeFseqRangeList *const list)
    ;

  /**
   * Returns true if there is another tape file sequence number in the
   * sequence.
   */
  bool hasMore() const noexcept;

  /**
   * Returns the next  tape file sequence number in the sequence, or throws an
   * exception if there isn't one.
   */
  uint32_t next() ;

  /**
   * Returns true if the sequence is finite, else false if it is infinite.
   */
  bool isFinite() const noexcept;

  /**
   * Returns the total number of values the sequence could ever generate.  The
   * value returned by this method is not affected by calls to next().  This
   * method returns 0 if the total number of values is 0 or infinity.  The
   * isFinite() method can be used to distinguish between the two cases.
   */
  uint32_t totalSize() const noexcept;


private:

  /**
   * The list of tape file sequence ranges.
   */
  const TapeFseqRangeList *m_list;

  /**
   * Iterator pointing to the current range of tape file sequence numbers.
   */
  TapeFseqRangeList::const_iterator m_rangeItor;

  /**
   * True if the sequence is finite, else false if it is infinite.
   */
  bool m_isFinite;

  /**
   * The total number of values the sequence could ever generate.  The
   * value returned by this method is not affected by calls to next().  This
   * method returns 0 if the total number of values is 0 or infinity.  The
   * isFinite() method can be used to distinguish between the two cases.
   */
  uint32_t m_totalSize;

  /**
   * The current sequence of the tape file sequence numbers.
   */
  TapeFseqRangeSequence m_nbSequence;

}; // class TapeFseqRangeListSequence

} // namespace cta::tapeserver::readtp

/**
 * ostream << operator for cta::tapeserver::readtp::TapeFseqRangeList
 */
std::ostream &operator<<(std::ostream &os,
  const cta::tapeserver::readtp::TapeFseqRangeList &value);
