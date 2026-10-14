{{/*
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later

The pinned upstream chart calls this helper in both door and pool pods, passing the certificate hostname.
Extend its init sequence because it has no extraInitContainers setting.
Keep init-host-certs last: the caller appends its certificate volume mount to the last container.
*/}}
{{- define "dcache.certs.init" }}
        - name: wait-for-postgres
          image: postgres:17
          envFrom:
            - configMapRef:
                name: postgres-secret
          command: [sh, -ec]
          args:
            - |
              export PGPASSWORD="$POSTGRES_PASSWORD" PGCONNECT_TIMEOUT=5
              until psql -h chimera-postgresql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'SELECT 1'; do
                echo "Waiting for the dCache database"
                sleep 2
              done
        - name: wait-for-zookeeper
          image: busybox:1.37
          command: [sh, -ec]
          args:
            - |
              until nc -z -w 5 cells-zookeeper 2181; do
                echo "Waiting for ZooKeeper"
                sleep 2
              done
        - name: prepare-billing-topic
          image: apache/kafka:3.9.1
          command: [sh, -ec]
          args:
            - |
              until /opt/kafka/bin/kafka-topics.sh --bootstrap-server billing-kafka:9092 \
                --create --if-not-exists --topic billing --partitions 1 --replication-factor 1; do
                echo "Waiting for the Kafka billing topic"
                sleep 2
              done
        - name: init-host-certs
          image: docker.io/dcache/ci-init-cert:1.2
          securityContext:
            runAsUser: 0
            runAsGroup: 0
          env:
            - name: AUTOCA_URL
              value: https://ci.dcache.org/ca
          command: [sh, -c]
          args:
            - /run.sh ${AUTOCA_URL} {{ . }}
{{- end }}
