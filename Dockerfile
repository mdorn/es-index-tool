FROM python:3-alpine
WORKDIR /usr/src/es_index_tool
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH ../
ENV STAGE production

COPY es_index_tool/ .

CMD ["python", "cli.py"]
