"""Start the protected Transfermarkt API in its hosted deployment."""

import bottle
from cheroot import wsgi

import config
import routes  # noqa: F401 - registers the endpoint handlers
from api_key import ApiKeyMiddleware


def main():
    app = ApiKeyMiddleware(bottle.default_app())
    print(f"Protected Transfermarkt Scraper listening on port {config.PORT}")
    server = wsgi.Server(
        ("0.0.0.0", config.PORT),
        app,
        server_name="transfermarkt-scraper",
        numthreads=16,
    )
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
