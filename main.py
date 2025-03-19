"""
runs both files
"""
from etab_batching import main as main_etab
from legal_batching import main as main_legal
from utils import pipeline_messenger
import sys
import traceback

# set global settings here
live_service = True

run_etab = True

run_legal = True

if __name__ == '__main__':

    if run_legal:
        try:
            main_legal()
            pipeline_messenger(
                title='French Companies Data Transfer',
                text='Etab Pipeline has finished running',
                notification_type='pass'
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback_str = traceback.format_exception(exc_type, exc_value, exc_traceback)
            pipeline_messenger(
                title='Sirene Data Transfer (Legale) Notification',
                text=str(traceback_str),
                notification_type='fail'
            )

    if run_etab:
        try:
            main_etab()
            pipeline_messenger(
                title='Sirene Data Transfer (Etab) Notification',
                text='Etab Pipeline has finished running',
                notification_type='pass'
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback_str = traceback.format_exception(exc_type, exc_value, exc_traceback)
            pipeline_messenger(
                title='Sirene Data Transfer (Etab) Notification',
                text=str(traceback_str),
                notification_type='fail'
            )



