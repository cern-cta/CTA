{{- define "tpsrv.name" -}}
  {{ include "common.names.name" . }}
{{- end }}

{{- define "tpsrv.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "tpsrv.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "tpsrv.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "tpsrv.libraries" -}}
  {{- $libraryConfig := .Values.libraries }}
  {{- if ne (typeOf $libraryConfig) "string" -}}
    {{- $libraryConfig = $libraryConfig | toYaml }}
  {{- end }}
  {{- $libraryConfig -}}
{{- end }}

{{- define "tpsrv.tapeServers" -}}
  {{- $tapeServersConfig := .Values.tapeServers }}
  {{- if ne (typeOf $tapeServersConfig) "string" -}}
    {{- $tapeServersConfig = $tapeServersConfig | toYaml }}
  {{- end }}
  {{- $tapeServersConfig -}}
{{- end }}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.global.configuration.scheduler }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}

{{/* Check if systemd is enabled. Use global if available, otherwise fallback to local value. */}}
{{- define "tpsrv.useSystemd" -}}
{{- if .Values.global.useSystemd }}
  {{- .Values.global.useSystemd }}
{{- else }}
  {{- .Values.useSystemd }}
{{- end }}
{{- end }}

{{/*
    Helper to validate unique names and unique drives across tapeServers
*/}}
{{- define "validate.tapeServers" -}}
  {{- $uniqueNames := dict -}}
  {{- $uniqueDrives := dict -}}
  {{- $tapeServers := include "tpsrv.tapeServers" . | fromYaml -}}
  {{- range $name,$tapeServerConfig := $tapeServers }}
    {{- $drives := .driveNames -}}
    {{- if hasKey $uniqueNames $name }}
      {{- fail (printf "Duplicate tapeServer name found: %s. Names must be unique." $name) }}
    {{- else }}
      {{- $_ := set $uniqueNames $name true }}
    {{- end }}
    {{- range $drives }}
      {{- if hasKey $uniqueDrives . }}
        {{- fail (printf "Duplicate drive found: %s. Drives must be unique across all tapeServers." .) }}
      {{- else }}
        {{- $_ := set $uniqueDrives . true }}
      {{- end }}
    {{- end }}
  {{- end }}
{{- end }}
