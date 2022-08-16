from pathlib import Path
from google_cloud_pipeline_components.v1.dataflow import DataflowPythonJobOp

import kfp.dsl as dsl
import json

from kfp.v2 import compiler

# Required parameters
PROJECT_ID = 'ca-pubtex-ai-verification'
GCS_STAGING_DIR = 'gs://ca-pubtex-ai-verification-dataflow/kfp-ouptuts' # No ending slash# Optional parameters
EXPERIMENT_NAME = 'Dataflow - Launch Python'
OUTPUT_FILE = '{}/wc/wordcount.out'.format(GCS_STAGING_DIR)

root_dir = Path(__file__).resolve().parents[3]
components_dir = Path(__file__).resolve().parents[3].joinpath("components")

@dsl.pipeline(
    name='wordcount-pipeline',
    description='Dataflow launch python pipeline',
    pipeline_root="gs://ca-pubtex-ai-verification-vertexai",
)
def pipeline(
    python_file_path: str = f'{components_dir}/wordcount/wc.py',
    project_id: str = PROJECT_ID,
    location: str = "us-central1",
    staging_dir: str = GCS_STAGING_DIR,
    requirements_file_path: str = f'{root_dir}/requirements.txt'
):
    df_op = DataflowPythonJobOp(
        project = project_id, 
        location = location,
        python_module_path = python_file_path, 
        temp_location = staging_dir,
        requirements_file_path = requirements_file_path, 
        args = json.dumps([
            '--output', OUTPUT_FILE
        ]))


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path="wordcount_pipeline.json",
    )