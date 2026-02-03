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

FROM ubuntu:24.04 # Install Python 3.12 manually (Ubuntu 24.04 has 3.12 by default) 

RUN apt-get update && apt-get install -y \ python3 \ python3-pip \ python3-venv \ && rm -rf /var/lib/apt/lists/* # Set aliases so "python" works 
RUN ln -s /usr/bin/python3 /usr/bin/python

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace
COPY . /workspace

RUN python -m pip install --no-cache-dir --upgrade pip
RUN pip install -e .
