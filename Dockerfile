FROM quay.io/astronomer/astro-runtime:12.0.0-python-3.11

COPY ./utilities utilities
COPY __init__.py setup.py ./
RUN pip install -e .