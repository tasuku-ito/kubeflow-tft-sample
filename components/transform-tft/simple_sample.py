import logging
import argparse

import tensorflow as tf
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import tensorflow_transform.beam as tft_beam
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import schema_utils
from tensorflow_transform.beam import impl as beam_impl

raw_data = [
        {'x': 1, 'y': 1, 's': 'hello'},
        {'x': 2, 'y': 2, 's': 'world'},
        {'x': 3, 'y': 3, 's': 'hello'}
    ]

raw_data_metadata = dataset_metadata.DatasetMetadata(
    schema_utils.schema_from_feature_spec({
        'y': tf.io.FixedLenFeature([], tf.float32),
        'x': tf.io.FixedLenFeature([], tf.float32),
        's': tf.io.FixedLenFeature([], tf.string),
    }))

def preprocessing_fn(inputs):
    """Preprocess input columns into transformed columns."""
    import tensorflow_transform as tft
    x = inputs['x']
    y = inputs['y']
    s = inputs['s']
    x_centered = x - tft.mean(x)
    y_normalized = tft.scale_to_0_1(y)
    s_integerized = tft.compute_and_apply_vocabulary(s)
    x_centered_times_y_normalized = (x_centered * y_normalized)
    return {
        'x_centered': x_centered,
        'y_normalized': y_normalized,
        's_integerized': s_integerized,
        'x_centered_times_y_normalized': x_centered_times_y_normalized,
    }


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_dir',
        dest='output_dir',
        required=True,
        help='Output directory.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    # Ignore the warnings
    with beam.Pipeline(options=pipeline_options) as p:
        with beam_impl.Context(temp_dir=known_args.output_dir):
            input = p | beam.Create(raw_data)
            transformed_dataset, transform_fn = (  # pylint: disable=unused-variable
                (input, raw_data_metadata) | tft_beam.AnalyzeAndTransformDataset(
                    preprocessing_fn))

            transformed_data, transformed_metadata = transformed_dataset  # pylint: disable=unused-variable

            # Save the transform_fn to the output_dir
            _ = (
                transform_fn
                | 'WriteTransformFn' >> tft_beam.WriteTransformFn(known_args.output_dir))
            
            (
            transformed_data 
                | 'As String' >> beam.Map(lambda x: str(x))
                | 'Write text' >> beam.io.WriteToText(known_args.output_dir + "/outputs-")
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
