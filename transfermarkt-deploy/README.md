# CromosList Transfermarkt deployment

The scraper is an upstream Git submodule. Render clones it using the root `.gitmodules` file, then `transfermarkt-deploy/Dockerfile` copies the upstream sources and installs the local WSGI API-key middleware from this directory.

The Render Blueprint creates a public scraper service and generates `TRANSFERMARKT_API_KEY`. Only `/` and `/health` are unauthenticated for health checks. Data endpoints require `X-API-Key` or `Authorization: Bearer <key>`. The CromosList service receives the scraper URL and key through Render service references; the browser never sees the secret.

In Render, sync the Blueprint from the repository root. Review the generated secret in the scraper service's environment, then let the Blueprint link it to the CromosList service. Keep the key out of source control and frontend configuration.
