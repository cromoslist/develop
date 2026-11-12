"""WSGI middleware protecting the public Render scraper service."""

import hmac
import json
import os


class ApiKeyMiddleware:
    HEALTH_PATHS = {"/", "/health"}

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        path = environ.get("PATH_INFO", "/")
        if path in self.HEALTH_PATHS:
            return self.app(environ, start_response)

        required_key = os.environ.get("TRANSFERMARKT_API_KEY", "")
        if not required_key:
            return self._json_response(
                start_response,
                "503 Service Unavailable",
                {"error": "API key is not configured"},
            )

        supplied_key = environ.get("HTTP_X_API_KEY", "")
        if not supplied_key:
            authorization = environ.get("HTTP_AUTHORIZATION", "")
            if authorization.lower().startswith("bearer "):
                supplied_key = authorization[7:].strip()

        if not supplied_key or not hmac.compare_digest(
            required_key.encode("utf-8"), supplied_key.encode("utf-8")
        ):
            return self._json_response(
                start_response,
                "401 Unauthorized",
                {"error": "unauthorized"},
            )

        return self.app(environ, start_response)

    @staticmethod
    def _json_response(start_response, status, data):
        body = json.dumps(data).encode("utf-8")
        start_response(status, [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ])
        return [body]
