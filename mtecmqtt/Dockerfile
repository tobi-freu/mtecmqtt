FROM python:3.15-alpine

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

LABEL \
    org.opencontainers.image.title="MTECmqtt Bridge" \
    org.opencontainers.image.description="Read-only Modbus RTU-over-TCP to MQTT bridge for M-TEC Energy Butler inverters" \
    org.opencontainers.image.source="https://github.com/tobi-freu/mtecmqtt" \
    org.opencontainers.image.licenses="MIT"

# Minimale Laufzeit-Abhängigkeiten (kein gcc/musl-dev/git/jq Image-Bloat)
RUN apk add --no-cache bash dos2unix

# Deterministisch gepinnte Python-Bibliotheken installieren
RUN pip3 install \
    "pyyaml==6.0.2" \
    "requests==2.32.3" \
    "pymodbus==3.8.3" \
    "paho-mqtt==2.1.0"

# MTECmqtt von fest gepinntem Commit installieren (ohne ungeprüfte Dependency-Überschreibung)
ARG MTECMQTT_REF="71cdde40732e1216b2f5a37d71e9737a009914a5"
RUN pip3 install --no-deps "https://github.com/croedel/MTECmqtt/archive/${MTECMQTT_REF}.tar.gz"

WORKDIR /data
COPY registers_extended.yaml /registers_extended.yaml
COPY mtec_runner.py /mtec_runner.py
COPY run.sh /run.sh
RUN dos2unix /run.sh /mtec_runner.py /registers_extended.yaml && chmod +x /run.sh /mtec_runner.py

# Build-Time Selbsttest: Verifiziert Imports, Framer-Kompatibilität, MQTT-Callbacks und Guard-Logik
RUN python3 /mtec_runner.py --self-test

CMD [ "/run.sh" ]
