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

namespace cta {
namespace catalogue {

/**
 * Abstract class defining the interface to an iterator over a list of items
 * retrieved from the CTA catalogue.
 */
template<typename Item>
class CatalogueItorImpl {
public:
  /**
   * Destructor.
   */
  virtual ~CatalogueItorImpl() = default;

  /**
   * Returns true if a call to next would return another archive file.
   */
  virtual bool hasMore() = 0;

  /**
   * Returns the next item or throws an exception if there isn't one.
   */
  virtual Item next() = 0;

};  // class CatalogueItorImpl

}  // namespace catalogue
}  // namespace cta
