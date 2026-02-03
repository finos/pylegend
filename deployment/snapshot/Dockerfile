# Copyright 2025 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM debian:sid-slim

# 2. Replicate the environment setup (mimic official python images)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Explicitly install Python 3 and Pip
#    (Unlike official images, bare Debian doesn't have them by default)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    # Install git if your setup.py needs it for versioning
    git \
    # Clean up to keep image small
    && rm -rf /var/lib/apt/lists/*

# 4. Create a symlink so 'python' calls 'python3'
RUN ln -sf /usr/bin/python3 /usr/bin/python

# 5. Handle PEP 668 (Debian 12+ blocks global pip installs)
#    This allows you to install globally like you did in Alpine
ENV PIP_BREAK_SYSTEM_PACKAGES=1

WORKDIR /workspace
COPY . /workspace

# 6. Install dependencies
#    Now this will use pre-compiled 'manylinux' wheels (FAST)
RUN python -m pip install --no-cache-dir --upgrade pip
RUN pip install -e .
