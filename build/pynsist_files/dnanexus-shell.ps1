# Copyright (C) 2013-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not
#  use this file except in compliance with the License. You may obtain a copy
#  of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.
#
#
# Run this file in PowerShell to initialize DNAnexus environment vars.

# Set DNANEXUS_HOME to the location of this file
$script:THIS_PATH = $myinvocation.mycommand.path
$script:BASE_DIR = split-path (resolve-path "$THIS_PATH") -Parent
$env:DNANEXUS_HOME = $BASE_DIR

# Activate virtualenv containing dxpy
& ($env:DNANEXUS_HOME + "\dxenv\Scripts\activate.ps1")

# Add DNAnexus bin dir to PATH so dx-verify-file.exe and friends are available
$env:PATH = "$env:DNANEXUS_HOME/bin;" + $env:PATH

# Set utf-8 as default IO encoding
$env:PYTHONIOENCODING = "utf-8"

# Print banner
"DNAnexus CLI initialized. For help, run: 'dx help'"
""
"Client version: " + (dx --version)
""
