{
  "name": "NovaMart Airflow Pipeline",
  "image": "mcr.microsoft.com/devcontainers/python:3.11-bullseye",
  "features": {
    "ghcr.io/devcontainers/features/docker-in-docker:1": {},
    "ghcr.io/astronomer/devcontainer-features/astro-cli:1": {}
  },
  "forwardPorts": [8080],
  "portsAttributes": {
    "8080": {
      "label": "Airflow Web UI",
      "onAutoForward": "notify"
    }
  },
  "customizations": {
    "vscode": {
      "extensions": [
        "ms-python.python",
        "ms-azuretools.vscode-docker",
        "redhat.vscode-yaml"
      ]
    }
  },
  "postCreateCommand": "chmod -R 777 data sql || true",
  "postStartCommand": "astro dev start -n --wait 2m"
}