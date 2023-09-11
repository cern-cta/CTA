/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of
 * the GNU General Public Licence version 3 (GPL Version 3), copied verbatim in
 * the file "COPYING". You can redistribute it and/or modify it under the terms
 * of the GPL Version 3, or (at your option) any later version.
 *
 *               This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges
 * and immunities granted to it by virtue of its status as an Intergovernmental
 * Organization or submit itself to any jurisdiction.
 */

#pragma once

/** @brief Interface class that provides plugin's metadata and instantiate
 * exported classes */
struct IPluginFactory {
  /** Get Plugin Name */
  virtual const char *Name() const = 0;
  /** Get Plugin Version */
  virtual const char *Version() const = 0;

  virtual size_t NumberOfClasses() const = 0;
  virtual const char *GetClassName(size_t index) const = 0;

  /** Instantiate a class from its name */
  virtual void *Factory(const char *className) const = 0;
};
