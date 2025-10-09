# Use Ubuntu 24.04 as base
FROM ubuntu:24.04

# Avoid interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive

ENV TZ=America/Argentina/Buenos_Aires

RUN apt-get update && apt-get install -y tzdata \
 && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
 && echo $TZ > /etc/timezone

# 1. Install system dependencies (GDAL, Python, Node prerequisites)
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    software-properties-common \
    git \
    gdal-bin=3.8.4+dfsg-3ubuntu3 \
    libgdal-dev \
    python3.12 \
    python3-setuptools \
    python3.12-dev \
    python3.12-venv \
    python3-pip \
    python3-gdal \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 2. Set Python 3.12 as default
# RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 \
#     && update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1 \
#     && python3 -m pip install --upgrade pip setuptools wheel

# 3. Install Node.js 20.x (official NodeSource script)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs

# 4. Environment variables (optional, helpful for GDAL-based Python packages)
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal



# 5. Create app directory

COPY app ./app

WORKDIR /app
COPY install_dependencies.sh ./
RUN bash install_dependencies.sh
# RUN npm install

# 6. Copy your project (optional — adjust for your project structure)

WORKDIR /py
COPY py/requirements.txt  ./
RUN python3 -m pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt
RUN pip install --no-cache-dir --force-reinstall 'GDAL[numpy]==3.8.4'
ADD py/*.py ./

WORKDIR /
COPY rest.js stop.js index.js crud_procedures.js ./
# COPY . .

WORKDIR /public
COPY public ./

WORKDIR /views
COPY views ./

# WORKDIR /config
# COPY config ./

WORKDIR /logs
RUN touch memUsage.log

WORKDIR /sessions

WORKDIR /

ENV PORT=3000

ENV NODE_ENV=production

EXPOSE 3000

# Default command (can override with `docker run ... <cmd>`)
CMD [ "node", "app/rest.mjs" ]
