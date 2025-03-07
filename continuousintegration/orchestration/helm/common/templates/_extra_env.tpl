// Shamelessly copied from https://gitlab.cern.ch/eos/eos-charts/-/tree/master/utils/templates
{{- /*
Output YAML formatted EnvVar entries for use in a containers.extraEnv field.
  Credits to JupyterHub chart -- https://github.com/jupyterhub/helm-chart
*/}}
{{- define "utils.extraEnv" -}}
{{- include "utils.extraEnv.withTrailingNewLine" . | trimSuffix "\n" }}
{{- end }}

{{- define "utils.extraEnv.withTrailingNewLine" -}}
{{- if . }}
{{- /* If extraEnv is a list, we inject it as it is. */}}
{{- if eq (typeOf .) "[]interface {}" }}
{{- . | toYaml }}

{{- /* If extraEnv is a map, we differentiate two cases: */}}
{{- else if eq (typeOf .) "map[string]interface {}" }}
{{- range $key, $value := . }}
{{- /*
    - If extraEnv.someKey has a map value, then we add the value as a YAML
      parsed list element and use the key as the name value unless its
      explicitly set.
*/}}
{{- if eq (typeOf $value) "map[string]interface {}" }}
{{- merge (dict) $value (dict "name" $key) | list | toYaml | println }}
{{- /*
    - If extraEnv.someKey has a string value, then we use the key as the
     .extraEnvironment variable name for the value.
*/}}
{{- else if eq (typeOf $value) "string" -}}
- name: {{ $key | quote }}
  value: {{ $value | quote | println }}
{{- else }}
{{- printf "?.extraEnv.%s had an unexpected type (%s)" $key (typeOf $value) | fail }}
{{- end }}
{{- end }} {{- /* end of range */}}
{{- end }}
{{- end }} {{- /* end of: if . */}}
{{- end }} {{- /* end of definition */}}

