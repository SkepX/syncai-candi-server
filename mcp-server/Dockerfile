FROM python:3.13-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN mkdir cardano-mcp
WORKDIR /cardano-mcp

COPY . /cardano-mcp/

RUN uv sync
EXPOSE 4324
CMD ["uv", "run", "server.py"]