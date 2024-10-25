{{- define "ctacli.name" -}}
  {{ include "common.names.name" . }}
{{- end }}

{{- define "ctacli.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctacli.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctacli.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctacli.volumes" -}}
  {{ include "common.volumes" . }}
{{- end -}}

{{- define "ctacli.volumeMounts" -}}
  {{ include "common.volumeMounts" . }}
{{- end -}}
