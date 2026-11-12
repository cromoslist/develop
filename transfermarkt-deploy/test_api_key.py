import json
import os
import unittest
from unittest.mock import patch

from api_key import ApiKeyMiddleware


class ApiKeyMiddlewareTests(unittest.TestCase):
    @staticmethod
    def upstream(_environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"upstream"]

    def invoke(self, path, headers=None):
        captured = {}

        def start_response(status, response_headers):
            captured["status"] = status
            captured["headers"] = dict(response_headers)

        environ = {"PATH_INFO": path}
        environ.update(headers or {})
        app = ApiKeyMiddleware(self.upstream)
        captured["body"] = b"".join(app(environ, start_response))
        return captured

    @patch.dict(os.environ, {"TRANSFERMARKT_API_KEY": "secret-key"})
    def test_health_is_public(self):
        result = self.invoke("/health")
        self.assertEqual(result["status"], "200 OK")
        self.assertEqual(result["body"], b"upstream")

    @patch.dict(os.environ, {"TRANSFERMARKT_API_KEY": "secret-key"})
    def test_data_endpoint_accepts_api_key_header(self):
        result = self.invoke("/players/profile", {"HTTP_X_API_KEY": "secret-key"})
        self.assertEqual(result["status"], "200 OK")
        self.assertEqual(result["body"], b"upstream")

    @patch.dict(os.environ, {"TRANSFERMARKT_API_KEY": "secret-key"})
    def test_data_endpoint_accepts_bearer_token(self):
        result = self.invoke(
            "/players/profile",
            {"HTTP_AUTHORIZATION": "Bearer secret-key"},
        )
        self.assertEqual(result["status"], "200 OK")

    @patch.dict(os.environ, {"TRANSFERMARKT_API_KEY": "secret-key"})
    def test_data_endpoint_rejects_missing_or_wrong_key(self):
        for headers in ({}, {"HTTP_X_API_KEY": "wrong-key"}, {"HTTP_X_API_KEY": "clave-incorrecta-é"}):
            with self.subTest(headers=headers):
                result = self.invoke("/players/profile", headers)
                self.assertEqual(result["status"], "401 Unauthorized")
                self.assertEqual(json.loads(result["body"]), {"error": "unauthorized"})

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_key_fails_closed(self):
        result = self.invoke("/players/profile")
        self.assertEqual(result["status"], "503 Service Unavailable")
        self.assertEqual(json.loads(result["body"]), {"error": "API key is not configured"})


if __name__ == "__main__":
    unittest.main()
