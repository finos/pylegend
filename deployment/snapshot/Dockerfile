# Copyright 2025 Goldman Sachs
# ... (License header remains the same) ...

# 1. Use Ubuntu 24.04 (Noble Numbat)
#    This has Python 3.12 pre-installed as the system default.
FROM ubuntu:24.04

# 2. Set environment variables to non-interactive
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Install Python tools
#    'python3' here IS Python 3.12.
#    'python-is-python3' creates the 'python' -> 'python3' symlink for you.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    python-is-python3 \
    git \
    ca-certificates \
    # Optional: Upgrade OS packages immediately to patch any CVEs
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

# 4. Handle PEP 668 (System-wide install restriction)
#    Ubuntu 24.04 blocks global 'pip install' by default. 
#    This variable overrides that protection so it behaves like your old Alpine image.
ENV PIP_BREAK_SYSTEM_PACKAGES=1

WORKDIR /workspace
COPY . /workspace

# 5. Install dependencies
#    This uses 'glibc' so it pulls pre-compiled wheels (Fast build).
RUN pip install -e .
