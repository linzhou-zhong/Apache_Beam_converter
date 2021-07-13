import argparse
import logging
from io import StringIO
from pathlib import Path
import pandas as pd
import os
from typing import List

import apache_beam as beam
from apache_beam.io import ReadAllFromText
from apache_beam.transforms import Reshuffle
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from core import FileSystem
from urllib.parse import urlparse


class ConvertTabularToJson(beam.DoFn):
    def __init__(
        self,
        headers: List[str],
        delimiter: str,
    ):
        super().__init__()
        self._headers = headers
        self._delimiter = delimiter

    def process(self, line, *args, **kwargs):
        _dicts = pd.read_csv(
            StringIO(line),
            names=self._headers,
            delimiter=self._delimiter,
            dtype="str",
            keep_default_na=False,
        ).to_dict(orient="records")

        for _dict in _dicts:
            yield _dict


def run(argv=None, save_main_session=True):
    """Main entry point, defines and runs the wordcount pipeline"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default=os.path.join(Path(__file__).parent.parent, "source/input"),
        help="Input file to process.",
    )

    parser.add_argument(
        "--pattern",
        dest="pattern",
        default="*.csv",
        help="Pattern to match files",
    )

    parser.add_argument(
        "--output",
        dest="output",
        default=os.path.join(
            Path(__file__).parent.parent, "source/output", "output"
        ),
        help="Output file to write results to.",
    )

    parser.add_argument(
        "--headers",
        dest="headers",
        default=os.path.join(
            Path(__file__).parent.parent, "config", "headers.txt"
        ),
        help="Headers of csv file",
    )

    parser.add_argument(
        "--delimiter",
        dest="delimiter",
        default=",",
        help="Delimiter of original file",
    )

    parser.add_argument(
        "--skip",
        dest="skip",
        default=1,
        help="Skip the lines from beginning of file",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with open(known_args.headers, "r") as content:
        headers = content.read().splitlines()

    protocol = urlparse(known_args.input).scheme

    fs = FileSystem(protocol=protocol)

    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection
        files = p | "Get Files List" >> beam.Create(
            fs.get_files(path=known_args.input, file_pattern=known_args.pattern)
        )

        lines = (
            files
            | ReadAllFromText(skip_header_lines=known_args.skip)
            | "Reshuffle" >> beam.transforms.Reshuffle()
        )

        output = lines | "Convert to Json" >> beam.ParDo(
            ConvertTabularToJson(headers=headers, delimiter=known_args.delimiter)
        )

        output | "Write" >> WriteToText(known_args.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
