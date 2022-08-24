"""import"""
from pathlib import Path
from google_cloud_pipeline_components.v1.dataflow import DataflowPythonJobOp

import json

import kfp
from kfp.v2 import compiler

"""components"""
module_dir = Path(__file__).resolve().parents[2].joinpath("components")
transform_tft_component_yaml_path = module_dir.joinpath(
    "transform-tft/transform-tft.yaml").resolve().as_posix()
transform_tft = kfp.components.load_component_from_file(transform_tft_component_yaml_path)

"""setteing"""
project_id = "ca-pubtex-ai-verification"
PIPELINE_ROOT = "gs://ca-pubtex-ai-verification-dataflow/pipeline_root"

"""define pipeline"""
@kfp.dsl.pipeline(
    name="transform-tft-pipeline",
    description="A simple_sample pipeline",
    pipeline_root=PIPELINE_ROOT,
)
def pipeline(
    project: str = project_id,
    region: str = "us-central1",
    temp_location: str = "gs://ca-pubtex-ai-verification-dataflow/transform-tft/tmp",
    setup_file: str = "/transform-tft/setup.py",
    output_dir: str = "gs://ca-pubtex-ai-verification-dataflow/tmp/output_tft",
):
    df_op = transform_tft(
        project = project, 
        region = region,
        temp_location = temp_location,
        setup_file = setup_file,
        output_dir = output_dir,
    )
    

if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path="transform-tft-pipeline.json",
    )