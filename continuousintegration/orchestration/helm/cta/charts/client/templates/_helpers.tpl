{{- define "client.name" -}}
  {{ include "common.names.name" . }}
{{- end }}

{{- define "client.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "client.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "client.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "client.volumes" -}}
  {{ include "common.volumes" . }}
{{- end -}}

{{- define "client.volumeMounts" -}}
  {{ include "common.volumeMounts" . }}
{{- end -}}
