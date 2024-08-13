"""
runs both files
"""
from etab_main import run_etab
from legal_main import run_legal
from utils import pipeline_messenger
import sys
import traceback

if __name__ == '__main__':
    try:
        run_etab()
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

    try:
        run_legal()
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

