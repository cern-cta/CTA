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

{{/*
    Helper to validate unique names and unique drives across tapeServers
*/}}
{{- define "validate.tapeServers" -}}
  {{- $uniqueNames := dict -}}
  {{- $uniqueDrives := dict -}}
  {{- $tapeServers := include "tpsrv.tapeServers" . | fromYaml -}}
  {{- range $name,$tapeServerConfig := $tapeServers }}
    {{- $drives := .drives -}}
    {{- if hasKey $uniqueNames $name }}
      {{- fail (printf "Duplicate tapeServer name found: %s. Names must be unique." $name) }}
    {{- else }}
      {{- $_ := set $uniqueNames $name true }}
    {{- end }}
    {{- range $drives }}
      {{- if hasKey $uniqueDrives .name }}
        {{- fail (printf "Duplicate drive found: %s. Drives must be unique across all tapeServers." .) }}
      {{- else }}
        {{- $_ := set $uniqueDrives .name true }}
      {{- end }}
    {{- end }}
  {{- end }}
{{- end }}

{{/*
    Helper to get a list of all unique drives across tapeServers as a list.
*/}}
{{- define "tpsrv.allDrives" -}}
  {{- $uniqueDrives := dict -}}
  {{- $tapeServers := include "tpsrv.tapeServers" . | fromYaml -}}
  {{- range $name, $tapeServerConfig := $tapeServers }}
    {{- range $drive := $tapeServerConfig.drives }}
      {{- if not (hasKey $uniqueDrives $drive.name) }}
        {{- $_ := set $uniqueDrives $drive.name $drive.device }}
      {{- end }}
    {{- end }}
  {{- end }}
  {{- $drivesList := list }}
  {{- range $name, $device := $uniqueDrives }}
    {{- $drive := dict "name" $name "device" $device }}
    {{- $drivesList = append $drivesList $drive }}
  {{- end }}
  {{- toYaml $drivesList }}
{{- end }}
