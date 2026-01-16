{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "rmcd.fullname" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "rmcd.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "rmcd.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "rmcd.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "rmcd.slugify" -}}
{{- . | lower | regexReplaceAll "[^a-z0-9-]+" "-" | trimAll "-" | trunc 63 -}}
{{- end -}}
