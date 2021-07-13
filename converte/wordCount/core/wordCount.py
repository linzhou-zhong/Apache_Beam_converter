import argparse
import logging
from pathlib import Path
import re
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class WordExtractingDoFn(beam.DoFn):
    """Parce each line of input text into words"""

    def process(self, element):
        """Returns an iterator over the words of this element

        The element is a line of text. If the line is blank, note that, too

        Args:
            element: the element being processed.

        Returns:
            The processed element.
        """
        return re.findall(r"[a-zA-Z']+", element, re.UNICODE)


def run(argv=None, save_main_session=True):
    """Main entry point, defines and runs the wordcount pipeline"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default=os.path.join(
            Path(__file__).parent.parent, "source/input", "input.txt"
        ),
        help="Input file to process.",
    )

    parser.add_argument(
        "--output",
        dest="output",
        default=os.path.join(
            Path(__file__).parent.parent, "source/output", "output"
        ),
        help="Output file to write results to.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection
        lines = p | "Read" >> ReadFromText(known_args.input)

        counts = (
            lines
            | "Split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
            | "PairWithOne" >> beam.Map(lambda x: (x, 1))
            | "GroupAndSum" >> beam.CombinePerKey(sum)
        )

        def format_result(word, count):
            return "%s: %d" % (word, count)

        output = counts | "Format" >> beam.MapTuple(format_result)

        output | "Write" >> WriteToText(known_args.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
