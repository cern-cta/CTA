{{- define "init.name" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "init.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "init.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "init.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "init.tapeServers" -}}
  {{- $tapeServersConfig := .Values.tapeServers }}
  {{- if ne (typeOf $tapeServersConfig) "string" -}}
    {{- $tapeServersConfig = $tapeServersConfig | toYaml }}
  {{- end }}
  {{- $tapeServersConfig -}}
{{- end }}

{{- define "init.uniqueLibraryDevices" -}}
  {{- $uniqueDevices := dict -}}
  {{- $tapeServers := include "init.tapeServers" . | fromYaml -}}
  {{- range $tapeServers }}
    {{- $device := .libraryDevice -}}
    {{- $type := .libraryType -}}

    {{- if not (hasKey $uniqueDevices $device) }}
      {{- $_ := set $uniqueDevices $device $type }}
    {{- end }}
  {{- end }}
  {{- range $device, $type := $uniqueDevices }}
    {{ $device }}: {{ $type }}
  {{- end }}
{{- end }}
