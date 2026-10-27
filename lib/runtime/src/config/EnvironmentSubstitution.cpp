/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "runtime/config/EnvironmentSubstitution.hpp"

#include "common/exception/UserError.hpp"

#include <algorithm>
#include <cstdlib>
#include <optional>

namespace cta::runtime {
namespace {

bool isNameStart(char c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
}

bool isNameCharacter(char c) {
  return isNameStart(c) || (c >= '0' && c <= '9');
}

bool isReferenceCharacter(char c) {
  return (c >= ' ' && c <= '~' && c != '}') || c == '\t';
}

struct Reference {
  std::string_view name;
  std::optional<std::string_view> fallback;
};

Reference parseReference(std::string_view reference, std::size_t offset) {
  std::size_t nameEnd = 0;
  if (!reference.empty() && isNameStart(reference.front())) {
    nameEnd = 1;
    while (nameEnd < reference.size() && isNameCharacter(reference[nameEnd])) {
      ++nameEnd;
    }
  }
  const auto suffix = reference.substr(nameEnd);
  const bool hasFallback = suffix.starts_with(":-");
  if (nameEnd == 0 || (!suffix.empty() && !hasFallback)) {
    // Avoid including scalar contents or defaults, which may contain credentials, in diagnostics.
    throw cta::exception::UserError("Invalid environment substitution at byte " + std::to_string(offset)
                                    + "; expected ${NAME}, optionally with :-fallback.",
                                  false);
  }
  return {reference.substr(0, nameEnd), hasFallback ? std::optional {suffix.substr(2)} : std::nullopt};
}

// Resolve one escape-free segment. Escape boundaries can interrupt an otherwise valid reference.
void substituteSegment(std::string_view input, std::size_t offset, bool errorOnMissing, std::string& output) {
  std::size_t cursor = 0;
  while (cursor < input.size()) {
    const auto start = input.find("${", cursor);
    if (start == std::string_view::npos) {
      output.append(input.substr(cursor));
      break;
    }
    output.append(input.substr(cursor, start - cursor));

    const auto end = input.find('}', start + 2);
    if (end == std::string_view::npos) {
      output.append(input.substr(start));
      break;
    }
    const auto reference = input.substr(start + 2, end - start - 2);
    if (reference.empty() || !std::all_of(reference.begin(), reference.end(), isReferenceCharacter)) {
      // Not a SUBSTITUTION-REF under the specification's ABNF; keep scanning for subsequent references.
      output.append("${");
      cursor = start + 2;
      continue;
    }

    const auto parsed = parseReference(reference, offset + start);
    const std::string name(parsed.name); // parsed.name is a string_view
    const char* value = std::getenv(name.c_str());
    if (value != nullptr && *value != '\0') {
      output.append(value);
    } else if (parsed.fallback) {
      output.append(*parsed.fallback);
    } else if (value == nullptr && errorOnMissing) {
      throw cta::exception::UserError("Environment variable '" + name + "' is not defined and has no fallback.", false);
    }
    cursor = end + 1;
  }
}

}  // namespace

std::string substituteEnvironmentVariables(std::string_view input, bool errorOnMissing) {
  std::string output;
  output.reserve(input.size());

  // The specification gives $$ precedence over reference matching, including inside defaults.
  std::size_t cursor = 0;
  while (true) {
    const auto escape = input.find("$$", cursor);
    if (escape == std::string_view::npos) {
      substituteSegment(input.substr(cursor), cursor, errorOnMissing, output);
      return output;
    }
    substituteSegment(input.substr(cursor, escape - cursor), cursor, errorOnMissing, output);
    output.push_back('$');
    cursor = escape + 2;
  }
}

}  // namespace cta::runtime
