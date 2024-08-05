"""
runs both files
"""
from etab_main import run_etab
from legal_main import run_legal
from utils import pipeline_messenger

if __name__ == '__main__':
    try:
        run_etab()
        pipeline_messenger(
            title='French Companies Data Transfer',
            text='Etab Pipeline has finished running',
            notification_type='pass'
        )
    except Exception as e:
        print(e)
    try:
        run_legal()
        pipeline_messenger(
            title='French Companies Data Transfer',
            text='Etab Pipeline has finished running',
            notification_type='pass'
        )
    except Exception as e:
        print(e)

