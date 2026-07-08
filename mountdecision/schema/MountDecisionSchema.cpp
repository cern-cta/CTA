/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mountdecision/schema/MountDecisionSchema.hpp"

namespace cta::mountdecision {

const std::string MountDecisionSchema::sql = R"SQL(
CREATE TABLE MOUNT_DECISION_KV(
  KEY_NAME VARCHAR(255) CONSTRAINT MOUNT_DECISION_KV_KEY_NN NOT NULL,
  VALUE VARCHAR(255) CONSTRAINT MOUNT_DECISION_KV_VALUE_NN NOT NULL,
  CONSTRAINT MOUNT_DECISION_KV_PK PRIMARY KEY(KEY_NAME)
);
)SQL";

}  // namespace cta::mountdecision
