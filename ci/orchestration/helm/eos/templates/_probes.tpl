{{/*
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later

Override the upstream QDB helper until its readiness timing is configurable.
Probe promptly so MGM initialization can reach QDB through its headless Service.
*/}}
{{- define "qdb.readinessProbe" -}}
{{- $readinessEnabled := true -}}
{{- if .Values.probes -}}
  {{- $readinessEnabled = dig "readiness" $readinessEnabled .Values.probes -}}
{{- end }}
{{- if .Values.global -}}
  {{- $readinessEnabled = dig "probes" "qdb_readiness" $readinessEnabled .Values.global -}}
{{- end }}
{{- if $readinessEnabled -}}
readinessProbe:
  exec:
    command:
    - /usr/bin/redis-cli
    - -p
    - "7777"
    - ping
  initialDelaySeconds: 0
  periodSeconds: {{ dig "readinessPeriodSeconds" 1 .Values.probes }}
  timeoutSeconds: 1
  successThreshold: 1
  failureThreshold: 3
{{- end }}
{{- end }}
