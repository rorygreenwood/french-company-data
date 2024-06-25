import mysql.connector
import os
import requests
import json

def connect_preprod():
    db = mysql.connector.connect(
        host=os.environ.get('HOST'),
        user=os.environ.get('ADMINUSER'),
        passwd=os.environ.get('ADMINPASS'),
        database=os.environ.get('DATABASE'),
    )

    cursor = db.cursor()
    return cursor, db

uri = f'mysql://{os.environ.get("ADMINUSER")}:{os.environ.get("ADMINPASS")}@{os.environ.get("PREPRODHOST")}:3306/{os.environ.get("DATABASE")}'

def pipeline_messenger(title, text, hexcolour_value):
    messenger_colours = {
        'pass': '#00c400',
        'fail': '#c40000',
        'notification': '#0000c4'
    }
    url = "https://tdworldwide.webhook.office.com/webhookb2/d5d1f4d1-2858-48a6-8156-5abf78a31f9b@7fe14ab6-8f5d-4139-84bf-cd8aed0ee6b9/IncomingWebhook/76b5bd9cd81946338da47e0349ba909d/c5995f3f-7ce7-4f13-8dba-0b4a7fc2c546"
    payload = json.dumps({
        "@type": "MessageCard",
        "themeColor": messenger_colours[hexcolour_value],
        "title": title,
        "text": text,
        "markdown": True
    })
    headers = {
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)


