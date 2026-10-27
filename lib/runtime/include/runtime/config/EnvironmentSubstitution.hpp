/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <string_view>

namespace cta::runtime {

/**
 * @brief Substitute environment variable references in text using syntax based on OpenTelemetry.
 *
 * Supports ${NAME}.
 * Use ${NAME:-fallback} to supply a fallback for an undefined or empty variable.
 * References require braces: $NAME is left as the literal text "$NAME".
 * $$ produces a literal dollar, so $${NAME} becomes the literal text "${NAME}" without looking up NAME.
 * Substitution is not recursive: references in environment variable values or fallback text are not substituted.
 * Text that does not form a substitution reference (such as an unclosed ${NAME) is preserved.
 * Prefixes are not supported.
 *
 * The process environment must not be modified concurrently with this call.
 *
 * @param input Text containing environment variable references.
 * @param errorOnMissing If true, throw for an undefined variable without a fallback; otherwise insert an empty string.
 * An explicitly empty variable is not missing, and an explicit fallback (even empty) prevents this error.
 * This flag does not affect errors for invalid substitution syntax or unsupported prefixes.
 * @return The text after substitution. Errors throw without returning partial output.
 * @throws cta::exception::UserError If a substitution reference has invalid syntax (e.g. ${1NAME}) or an unsupported
 * prefix (e.g. ${sys:NAME}), regardless of errorOnMissing; also if errorOnMissing is true and a referenced variable
 * is undefined with no fallback.
 * @see https://opentelemetry.io/docs/specs/otel/configuration/data-model/#environment-variable-substitution
 */
std::string substituteEnvironmentVariables(std::string_view input, bool errorOnMissing = false);

}  // namespace cta::runtime
