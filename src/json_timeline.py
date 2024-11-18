# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import shutil
import subprocess
import time
from uuid import uuid4

from openrelik_worker_common.utils import (
    create_output_file,
    get_input_files,
    task_result,
)

from .app import celery

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-hayabusa.tasks.json_timeline"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Hayabusa JSON timeline",
    "description": "Windows event log triage with JSONL output",
}


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def json_timeline(
    self,
    pipe_result=None,
    input_files=[],
    output_path=None,
    workflow_id=None,
    task_config={},
) -> str:
    # Get input files from the pipeline or arguments
    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []

    # Create an output file for JSONL
    output_file = create_output_file(
        output_path,
        filename="Hayabusa_JSONL_timeline",
        file_extension="jsonl",
        data_type="openrelik:worker:hayabusa:file:jsonl",
    )

    # Create temporary directory and hard link files for processing
    temp_dir = os.path.join(output_path, uuid4().hex)
    os.mkdir(temp_dir)
    for file in input_files:
        filename = os.path.basename(file.get("path"))
        os.link(file.get("path"), f"{temp_dir}/{filename}")

    # Command to execute Hayabusa with JSONL output and super-verbose mode
    command = [
        "/hayabusa/hayabusa",
        "json-timeline",          # Command for JSON timeline
        "--ISO-8601",
        "--UTC",
        "--no-wizard",
        "--quiet",
        "--JSONL-output",         # Specify JSONL output format
        "--profile",
        "super-verbose",        # Add super-verbose flag
        "--output",
        output_file.path,
        "--directory",
        temp_dir,
    ]

    # Execute the command and monitor progress
    INTERVAL_SECONDS = 2
    process = subprocess.Popen(command)
    while process.poll() is None:
        self.send_event("task-progress", data=None)
        time.sleep(INTERVAL_SECONDS)

    # Remove temporary directory after processing
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    # Add output file metadata to results
    output_files.append(output_file.to_dict())

    # Ensure at least one output file was created
    if not output_files:
        raise RuntimeError("Hayabusa didn't create any output files")

    # Return task results to OpenRelik
    return task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=" ".join(command),
    )
