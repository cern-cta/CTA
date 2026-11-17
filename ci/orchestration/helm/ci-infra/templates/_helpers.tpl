{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "ci-infra.kerberos.fullname" -}}
  {{- printf "%s-%s" .Release.Name "kdc" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
