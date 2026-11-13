FROM python:3.13-slim

# BlueOS Extension metadata (version and readme come last, after the slow steps, so a new version keeps their cache)
LABEL permissions='{\
  "ExposedPorts": {\
    "9650/tcp": {},\
    "9651/tcp": {}\
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

# Each of the ~50 threads (polls, container logs) otherwise gets its own malloc arena, and memory grew for as long as the
# logger ran while Python's own heap stayed flat. Two arenas keep it flat (measured: ~84 MB, unchanged over 25 min).
ENV MALLOC_ARENA_MAX=2

COPY requirements.txt .
# zstandard, lz4 and cffi ship no armv7 wheels, so the compiler is needed there (and removed in the same layer).
# The Debian mirrors drop connections now and then, so let apt retry.
RUN apt-get -o Acquire::Retries=10 update \
    && apt-get -o Acquire::Retries=10 install -y --no-install-recommends gcc libc6-dev libffi-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y gcc libc6-dev libffi-dev && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

ARG VERSION=0.0.0
ENV VERSION=${VERSION}
LABEL version="${VERSION}"
LABEL readme="https://raw.githubusercontent.com/rafaellehmkuhl/blueos-complete-data-logger/${VERSION}/README.md"

COPY main.py index.html player.html ./

CMD ["python", "-u", "main.py"]
