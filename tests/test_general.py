import unittest
from utils import pipeline_messenger


class TestPipelineMessengerWorks(unittest.TestCase):
    def main(self):
        pipeline_messenger(
            title='testcase',
            text='testing french data transfer pipeline messenger',
            notification_type='notification'
        )
