FROM ubuntu:latest
LABEL authors="T199811E"

RUN pip install -r requirements.txt

ENTRYPOINT ["top", "-b"]
CMD python etab_main.py