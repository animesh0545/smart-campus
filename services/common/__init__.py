"""Shared library for the Smart Campus microservices.

Each service is its own container image and deployment. This package only
holds cross-cutting code (config, logging, metrics, gRPC/Kafka/SQS helpers,
the consistent-hash ring), so it is copied into every image at build time.
"""
