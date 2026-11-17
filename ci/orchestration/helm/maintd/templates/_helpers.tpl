{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "ctamaintd.fullname" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "maintd.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "maintd.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "maintd.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}



{{/* Backend-specific fields are rejected by the other maintd build, even when disabled. */}}
{{- define "maintd.routines" -}}
{{- if eq .Values.global.scheduler.backend "postgres" -}}
{{- omit .Values.routines "garbageCollect" "queueCleanup" | toYaml -}}
{{- else -}}
{{- omit .Values.routines "userActiveQueueCleanup" "repackActiveQueueCleanup"
    "userPendingQueueCleanup" "repackPendingQueueCleanup" "schedulerMaintenanceCleanup" | toYaml -}}
{{- end -}}
{{- end -}}
