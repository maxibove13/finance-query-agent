FROM public.ecr.aws/lambda/python:3.12

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/

# Install deps from lock file (gets pre-built wheels), then the project itself
RUN uv export --no-dev --frozen --no-emit-project -o /tmp/requirements.txt && \
    uv pip install --system --no-cache -r /tmp/requirements.txt && \
    uv pip install --system --no-cache --no-deps .

# Lambda handler
CMD ["finance_query_agent.handler.handler"]
