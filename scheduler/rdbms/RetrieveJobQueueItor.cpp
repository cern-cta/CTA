/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RetrieveJobQueueItor.hpp"
#include "common/exception/Exception.hpp"

namespace cta::schedulerdb {

[[noreturn]] RetrieveJobQueueItor::RetrieveJobQueueItor()
{
   throw cta::exception::Exception("Not implemented");
}

const std::string &RetrieveJobQueueItor::qid() const
{
   throw cta::exception::Exception("Not implemented");
}

bool RetrieveJobQueueItor::end() const
{
   throw cta::exception::Exception("Not implemented");
}

void RetrieveJobQueueItor::operator++()
{
   throw cta::exception::Exception("Not implemented");
}

const common::dataStructures::RetrieveJob &RetrieveJobQueueItor::operator*() const
{
   throw cta::exception::Exception("Not implemented");
}

} // namespace cta::schedulerdb
