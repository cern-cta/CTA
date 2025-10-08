/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta {

/**
 * The path of a file in a remote storage system of the form
 * "scheme://path".
 */
class RemotePath {
public:
  /**
   * Constructor
   */
  RemotePath() = default;

  /**
   * Setter
   * @param raw
   */
  void setPath(const std::string &raw);

  /**
   * Constructor
   *
   * @param raw The raw path in the form "scheme:after_scheme"
   */
  explicit RemotePath(const std::string& raw);

  /**
   * Equals operator.
   */
  bool operator==(const RemotePath &rhs) const;

  /**
   * Less than operator
   */
  bool operator<(const RemotePath &rhs) const;

  /**
   * Returns true if the remote path is empty.
   */
  bool empty() const;

  /**
   * Returns the raw path in the form "scheme:after_schem".
   */
  const std::string &getRaw() const;

  /**
   * Returns the scheme part of the remote path.
   */
  const std::string &getScheme() const;

  /**
   * Returns the part of the remote path after the scheme as in the raw path
   * "scheme:after_scheme" would result in "after_scheme" being returned.
   *
   * @return The part of the remote path after the scheme as in the raw path
   * "scheme:after_scheme" would result in "after_scheme" being returned.
   */
  const std::string &getAfterScheme() const;

private:

  /**
   * The raw path in the form "scheme:after_scheme".
   */
  std::string m_raw;

  /**
   * The scheme part of the remote path.
   */
  std::string m_scheme;

  /**
   * The part of the remote path after the scheme.  For example the part after
   * the scheme for the raw path "scheme:after_scheme" would be "after_scheme".
   */
  std::string m_afterScheme;

}; // class RemotePath

} // namespace cta
