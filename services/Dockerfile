# One Dockerfile for every Python microservice (build context = repo root):
#   docker build -f services/Dockerfile --build-arg SERVICE=room-booking -t room-booking .
# Each service still ships as its own image, deployment and HPA.
FROM python:3.12-slim

ARG SERVICE
ENV SERVICE_NAME=${SERVICE} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/gen \
    GRPC_VERBOSITY=ERROR

WORKDIR /app

COPY services/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# gRPC stubs generated from the shared contract
COPY proto /app/proto
RUN mkdir -p /app/gen && python -m grpc_tools.protoc -I /app/proto \
      --python_out=/app/gen --grpc_python_out=/app/gen /app/proto/campus.proto

COPY services/common /app/common
COPY services/${SERVICE}/app.py /app/app.py

RUN useradd --uid 10001 --create-home app
USER 10001

EXPOSE 8000 50051
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
