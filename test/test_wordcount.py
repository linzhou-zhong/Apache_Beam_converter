import unittest
from apache_beam_utils.converter.wordCount import WordExtractingDoFn
from apache_beam.pipeline_test import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam as beam


class MyTestCase(unittest.TestCase):
    WORDS = ["hello hola", "hello"]
    EXPECTED_COUNT = [("hello", 2), ("hola", 1)]

    def test_count_words(self):
        with TestPipeline() as p:
            input = p | beam.Create(self.WORDS)

            output = (
                input
                | "Split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
                | "PairWithOne" >> beam.Map(lambda x: (x, 1))
                | "GroupAndSum" >> beam.CombinePerKey(sum)
            )

            assert_that(output, equal_to(self.EXPECTED_COUNT), label="CheckOutput")
