"""Module for launching Dataflow python jobs."""
import logging
from pathlib import Path
import re
import subprocess
from typing import get_type_hints
from dataclasses import dataclass
import argparse

from google_cloud_pipeline_components.proto import gcp_resources_pb2

from google.protobuf import json_format

# Set logging level to info
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)


@dataclass
class ComponentArguments:
    """Argument of the component. They are used by Dataflow launch command."""
    project: str
    region: str
    temp_location: str
    setup_file: str
    output_dir: str # TODO Dataflowの起動に必要な引数を書く


@dataclass
class ComponentOutputs:
    gcp_resources: str
    output_dir_path: str # TODO 後処理コンポーネントに渡したい引数を書く


@dataclass
class Artifacts:
    component_arguments: ComponentArguments
    component_outputs: ComponentOutputs

    @classmethod
    def arg_parser(cls) -> argparse.ArgumentParser:
        """Parse component argument and return as ComponentArguments."""
        parser = argparse.ArgumentParser()
        # generate argument parser based on ComponentArgument's definition
        for artifact in get_type_hints(cls).values():
            for arg_name, arg_type in get_type_hints(artifact).items():
                parser.add_argument(f"--{arg_name}", type=arg_type)

        return parser

    @classmethod
    def from_args(cls) -> "Artifacts":
        args = vars(cls.arg_parser().parse_args())

        artifacts = {}
        for key, artifact_cls in get_type_hints(cls).items():
            existed_keys = get_type_hints(artifact_cls).keys()
            filtered_vars = {k: v for k, v in args.items() if k in existed_keys}

            artifacts[key] = artifact_cls(**filtered_vars)
        # parse args and convert into PipelineArguments
        return cls(**artifacts)


def create_python_job(python_file_path: str,
                      gcp_resources: str,
                      artifact_arguments: ComponentArguments):

  job_id = None

  cmd = prepare_cmd(python_file_path, artifact_arguments)
  logger.info("Creating Dataflow Job...")
  sub_process = Process(cmd)
  for line in sub_process.read_lines():
    logger.info('DataflowRunner output: %s', line)
    job_id, location = extract_job_id_and_location(line)
    if job_id:
      logger.info('Found job id %s and location %s.', job_id, location)
      # Write the job proto to output.
      job_resources = gcp_resources_pb2.GcpResources()
      job_resource = job_resources.resources.add()
      job_resource.resource_type = 'DataflowJob'
      job_resource.resource_uri = f'https://dataflow.googleapis.com/v1b3/projects/{artifact_arguments.project}/locations/{artifact_arguments.region}/jobs/{job_id}'

      Path(artifacts.component_outputs.output_dir_path).parent.mkdir(parents=True, exist_ok=True)
      Path(artifacts.component_outputs.output_dir_path).write_text(str(artifacts.component_arguments.output_dir))
      
      with open(gcp_resources, 'w') as f:
        f.write(json_format.MessageToJson(job_resources))
      break
  if not job_id:
    raise RuntimeError(
        'No dataflow job was found when running the python file.')


def prepare_cmd(python_file_path: str, artifact_arguments: ComponentArguments):
  dataflow_args = [
      '--runner', 'DataflowRunner'
  ]
  args = []
  for flag, value in artifact_arguments.__dict__.items():
    args.append(f"--{flag}")
    args.append(value)

  return (['python3', '-u', python_file_path] + dataflow_args + args)


def extract_job_id_and_location(line):
  """Returns (job_id, location) from matched log."""
  job_id_pattern = re.compile(
      br'.*console.cloud.google.com/dataflow/jobs/(?P<location>[a-z|0-9|A-Z|\-|\_]+)/(?P<job_id>[a-z|0-9|A-Z|\-|\_]+).*'
  )
  matched_job_id = job_id_pattern.search(line or '')
  if matched_job_id:
    return (matched_job_id.group('job_id').decode(),
            matched_job_id.group('location').decode())
  return (None, None)


class Process:
  """Helper class to redirect the subprocess output."""

  def __init__(self, cmd):
    self._cmd = cmd
    self.process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        close_fds=True,
        shell=False)

  def read_lines(self):
    # stdout will end with empty bytes when process exits.
    for line in iter(self.process.stdout.readline, b''):
      # logger.info('subprocess: %s', line)
      yield line


if __name__ == '__main__':
    artifacts = Artifacts.from_args()
    create_python_job(
      python_file_path=Path(__file__).resolve().parents[0].joinpath("simple_sample.py"), # TODO モジュールへのファイルパスは適宜変更する
      gcp_resources=artifacts.component_outputs.gcp_resources,
      artifact_arguments=artifacts.component_arguments)

    # TODO outputsで必要なファイルへの書き出し
    Path(artifacts.component_outputs.output_dir_path).parent.mkdir(parents=True, exist_ok=True)
    Path(artifacts.component_outputs.output_dir_path).write_text(str(artifacts.component_arguments.output_dir))
