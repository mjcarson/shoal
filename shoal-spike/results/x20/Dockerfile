# X20's client: ScyllaDB's own Python driver, in a container of its own on europa, so no package is
# installed on the host. x20-lab.sh builds it as `x20-client` and removes it after
FROM python:3.12-slim
RUN pip install --no-cache-dir scylla-driver==3.29.12
COPY x20_client.py /x20_client.py
ENTRYPOINT ["python3", "/x20_client.py"]
