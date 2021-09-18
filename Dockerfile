FROM python:3.9-slim

WORKDIR /empatica-assignment

COPY src/ src/
COPY setup.py setup.py
COPY setup.cfg setup.cfg

RUN apt-get update -y && \
    apt-get install git wget unzip -y

RUN --mount=source=.git,target=.git,type=bind \
    pip install .

CMD ["python", "-m", "check_faulty_devices"]
