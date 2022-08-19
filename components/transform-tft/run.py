"""Module for launching Dataflow python jobs."""
import json
import logging
import os
import re
import subprocess
import tempfile
from typing import Tuple, Optional, get_type_hints
from dataclasses import dataclass
import argparse

from google.cloud import storage
from google_cloud_pipeline_components.proto import gcp_resources_pb2

from google.protobuf import json_format

# Set logging level to info
logging.basicConfig(level=logging.INFO)


@dataclass
class ComponentArguments:
    """Argument of the component. Note: Data Generator has no inputs."""
    project: str
    location: str
    python_module_path: str
    temp_location: str
    requirements_file_path: str
    args: str


@dataclass
class ComponentOutputs:
    gcp_resources: str


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


def create_python_job(python_module_path: str,
                      project: str,
                      gcp_resources: str,
                      location: str,
                      temp_location: str,
                      requirements_file_path: str = '',#ここはローカル側の環境構築用のrequirementsなので不要？
                      args: Optional[str] = '[]'):

  job_id = None
  if requirements_file_path: #ここはローカル側の環境構築用のrequirementsなので不要？
    install_requirements(requirements_file_path)
  args_list = []
  if args:
    args_list = json.loads(args)

  python_file_path = stage_file(python_module_path)
  # If --setup_file or --requirements_file are provided stage them locally.
  for idx, param in enumerate(args_list):
    if param in ('--requirements_file', '--setup_file'):
      args_list[idx + 1] = stage_file(args_list[idx + 1])
      logging.info('Staging %s at %s locally.', param, args_list[idx + 1])

  cmd = prepare_cmd(project, location, python_file_path, args_list,
                    temp_location)
  sub_process = Process(cmd)
  for line in sub_process.read_lines():
    logging.info('DataflowRunner output: %s', line)
    job_id, location = extract_job_id_and_location(line)
    if job_id:
      logging.info('Found job id %s and location %s.', job_id, location)
      # Write the job proto to output.
      job_resources = gcp_resources_pb2.GcpResources()
      job_resource = job_resources.resources.add()
      job_resource.resource_type = 'DataflowJob'
      job_resource.resource_uri = f'https://dataflow.googleapis.com/v1b3/projects/{project}/locations/{location}/jobs/{job_id}'

      with open(gcp_resources, 'w') as f:
        f.write(json_format.MessageToJson(job_resources))
      break
  if not job_id:
    raise RuntimeError(
        'No dataflow job was found when running the python file.')


def prepare_cmd(project_id, region, python_file_path, args, temp_location):
  dataflow_args = [
      '--runner', 'DataflowRunner', '--project', project_id, '--region', region,
      '--temp_location', temp_location
  ]

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


def install_requirements(requirements_file_path): #ここはローカル側の環境構築用のrequirementsなので不要？
  requirements_file_path = stage_file(requirements_file_path)
  subprocess.check_call(['pip', 'install', '-r', requirements_file_path])


def stage_file(gcs_path: str) -> str:
  _, blob_path = parse_blob_path(gcs_path)
  file_name = os.path.basename(blob_path)
  local_file_path = os.path.join(tempfile.mkdtemp(), file_name)
  download_blob(gcs_path, local_file_path)
  return local_file_path


def parse_blob_path(path) -> Tuple[str, str]:
  """Parse a gcs path into bucket name and blob name.
  Args:
    path: the path to parse.
  Returns:
    A Tuple consisting of (bucket name in the path, blob name in the path)
  Raises:
    ValueError if the path is not a valid gcs blob path.
  Example:
    `bucket_name, blob_name = parse_blob_path('gs://foo/bar')`
    `bucket_name` is `foo` and `blob_name` is `bar`
  """
  match = re.match('gs://([^/]+)/(.+)$', path)
  if match:
    return (match.group(1), match.group(2))
  raise ValueError('Path {} is invalid blob path.'.format(path))


def download_blob(source_blob_path, destination_file_path):
  """Downloads a blob from the bucket.
  Args:
      source_blob_path (str): the source blob path to download from.
      destination_file_path (str): the local file path to download to.
  """
  bucket_name, blob_name = parse_blob_path(source_blob_path)
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_name)

  dirname = os.path.dirname(destination_file_path)
  if not os.path.exists(dirname):
    os.makedirs(dirname)

  with open(destination_file_path, 'wb+') as f:
    blob.download_to_file(f)

  logging.info('Blob %s downloaded to %s.', source_blob_path,
               destination_file_path)


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
      logging.info('subprocess: %s', line)
      yield line

if __name__ == '__main__':
    
    artifacts = Artifacts.from_args()
    create_python_job(artifacts)