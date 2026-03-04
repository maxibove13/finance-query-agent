FROM public.ecr.aws/lambda/python:3.11

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/

# Install production dependencies only (no dev deps)
RUN uv pip install --system --no-cache .

# Lambda handler
CMD ["finance_query_agent.handler.handler"]
