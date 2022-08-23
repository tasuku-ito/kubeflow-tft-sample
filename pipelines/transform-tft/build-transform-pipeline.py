import google.cloud.aiplatform as aip

DISPLAY_NAME = "simple_sample"

aip.init(project="ca-pubtex-ai-verification", staging_bucket="gs://ca-pubtex-ai-verification-dataflow/")


job = aip.PipelineJob(
    display_name=DISPLAY_NAME,
    template_path="transform-tft-pipeline.json",#"gs://ca-pubtex-ai-verification-dataflow/transform-tft/transform-tft-pipeline.json",
    pipeline_root="gs://ca-pubtex-ai-verification-dataflow/pipeline_root",
)

job.run()