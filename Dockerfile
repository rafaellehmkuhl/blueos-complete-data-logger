FROM python:3.13-slim

ARG VERSION=0.0.0
ENV VERSION=${VERSION}

# BlueOS Extension metadata
LABEL version="${VERSION}"
LABEL permissions='{\
  "ExposedPorts": {\
    "9650/tcp": {}\
  },\
  "HostConfig": {\
    "NetworkMode": "host",\
    "Binds": [\
      "/usr/blueos/extensions/complete-data-logger:/app/logs",\
      "/var/logs/blueos/services:/var/logs/blueos/services:ro"\
    ]\
  }\
}'
LABEL authors='[\
    {\
        "name": "Rafael Araujo Lehmkuhl",\
        "email": "rafael.lehmkuhl93@gmail.com"\
    }\
]'
LABEL company='{\
  "about": "Blue Robotics - Underwater Robotics Made Accessible",\
  "name": "Blue Robotics",\
  "email": "rafael@bluerobotics.com"\
}'
LABEL readme="https://raw.githubusercontent.com/rafaellehmkuhl/blueos-complete-data-logger/${VERSION}/README.md"
LABEL links='{\
  "website": "https://github.com/rafaellehmkuhl/blueos-complete-data-logger",\
  "support": "https://github.com/rafaellehmkuhl/blueos-complete-data-logger/issues"\
}'
LABEL type="tool"
LABEL tags='[\
  "data-collection",\
  "logging",\
  "mcap"\
]'
LABEL requirements="core >= 1.4"

WORKDIR /app

COPY requirements.txt .
# zstandard and lz4 ship no armv7 wheels, so the compiler is needed there (and removed in the same layer).
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y gcc libc6-dev && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY main.py .

CMD ["python", "-u", "main.py"]
