# The inspiration is from Apache Airflow's API's hook

from __future__ import annotations

import time
import logging
from typing import Any, Callable

import requests
import json
from requests.auth import HTTPBasicAuth
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter


class AirbyteConnection:
    def __init__(self, host, username, password, api_version="v1", port=8000):
        self.host = host
        self.username = username
        self.password = password
        self.api_version = api_version
        self.port = 8000
    


class HttpHook:
    """
    Interact with HTTP servers.

    :param method: the API method to be called
    :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "HTTP"

    def __init__(
        self,
        auth_type: Any = HTTPBasicAuth,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
    ) -> None:
        super().__init__()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self.auth_type: Any = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.keep_alive_idle = tcp_keep_alive_idle
        self.keep_alive_count = tcp_keep_alive_count
        self.keep_alive_interval = tcp_keep_alive_interval

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(self, headers: dict[Any, Any] | None = None) -> requests.Session:
        """
        Returns http session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        """
        session = requests.Session()

        if self.connection:
            conn = self.connection

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            
            session.auth = self.auth_type(conn.username, conn.password)

        if headers:
            session.headers.update(headers)

        return session

    def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        method : str = "POST",
        **request_kwargs: Any,
    ) -> Any:
        r"""
        Performs the request

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :param request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        extra_options = extra_options or {}
        session = self.get_conn(headers)

        url = self.url_from_endpoint(endpoint)

        if self.tcp_keep_alive:
            keep_alive_adapter = TCPKeepAliveAdapter(
                idle=self.keep_alive_idle, count=self.keep_alive_count, interval=self.keep_alive_interval
            )
            session.mount(url, keep_alive_adapter)
        if method == "GET":
            # GET uses params
            req = requests.Request(method, url, params=data, headers=headers, **request_kwargs)
        elif method == "HEAD":
            # HEAD doesn't use params
            req = requests.Request(method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = requests.Request(method, url, data=data, headers=headers, **request_kwargs)

        prepped_request = session.prepare_request(req)
        logging.info("Sending '%s' to url: %s", method, url)
        return self.run_and_check(session, prepped_request, extra_options)

    def check_response(self, response: requests.Response) -> None:
        """
        Checks the status code and raise an Exception exception on non 2XX or 3XX
        status codes

        :param response: A requests response object
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            logging.error("HTTP error: %s", response.reason)
            logging.error(response.text)
            raise Exception(str(response.status_code) + ":" + response.reason)

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: dict[Any, Any],
    ) -> Any:
        """
        Grabs extra options like timeout and actually runs the request,
        checking for the result

        :param session: the session to be used to execute the request
        :param prepped_request: the prepared request generated in run()
        :param extra_options: additional options to be used when executing the request
            i.e. ``{'check_response': False}`` to avoid checking raising exceptions on non 2XX
            or 3XX status codes
        """
        extra_options = extra_options or {}

        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=extra_options.get("proxies", {}),
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify"),
            cert=extra_options.get("cert"),
        )

        # Send the request.
        send_kwargs: dict[str, Any] = {
            "timeout": extra_options.get("timeout"),
            "allow_redirects": extra_options.get("allow_redirects", True),
        }
        send_kwargs.update(settings)

        try:
            response = session.send(prepped_request, **send_kwargs)

            if extra_options.get("check_response", True):
                self.check_response(response)
            return response

        except requests.exceptions.ConnectionError as ex:
            logging.warning("%s Tenacity will retry to execute the operation", ex)
            raise ex

    def url_from_endpoint(self, endpoint: str | None) -> str:
        """Combine base url with endpoint"""
        if self.base_url and not self.base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            return self.base_url + "/" + endpoint
        return (self.base_url or "") + (endpoint or "")

    def test_connection(self):
        """Test HTTP Connection"""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)



class AirbyteHook(HttpHook):
    """
    Hook for Airbyte API

    :param airbyte_conn_id: Required. The name of the Airflow connection to get
        connection information for Airbyte.
    :param api_version: Optional. Airbyte API version.
    """

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    PENDING = "pending"
    FAILED = "failed"
    ERROR = "error"
    INCOMPLETE = "incomplete"

    def __init__(self, connection) -> None:
        super().__init__()
        self.connection = connection

    def wait_for_job(self, job_id: str | int, wait_seconds: float = 3, timeout: float | None = 3600) -> None:
        """
        Helper method which polls a job to check if it finishes.

        :param job_id: Required. Id of the Airbyte job
        :param wait_seconds: Optional. Number of seconds between checks.
        :param timeout: Optional. How many seconds wait for job to be ready.
            Used only if ``asynchronous`` is False.
        """
        state = None
        start = time.monotonic()
        while True:
            if timeout and start + timeout < time.monotonic():
                raise Exception(f"Timeout: Airbyte job {job_id} is not ready after {timeout}s")
            time.sleep(wait_seconds)
            try:
                job = self.get_job(job_id=(int(job_id)))
                state = job.json()["job"]["status"]
            except Exception as err:
                logging.info("Retrying. Airbyte API returned server error when waiting for job: %s", err)
                continue

            if state in (self.RUNNING, self.PENDING, self.INCOMPLETE):
                continue
            if state == self.SUCCEEDED:
                break
            if state == self.ERROR:
                raise Exception(f"Job failed:\n{job}")
            elif state == self.CANCELLED:
                raise Exception(f"Job was cancelled:\n{job}")
            else:
                raise Exception(f"Encountered unexpected state `{state}` for job_id `{job_id}`")

    def sync_connection(self, connection_id: str) -> Any:
        """
        Submits a job to a Airbyte server.

        :param connection_id: Required. The ConnectionId of the Airbyte Connection.
        """
        response = self.run(
            endpoint=f"api/{self.connection.api_version}/connections/sync",
            json={"connectionId": connection_id},
            headers={"accept": "application/json"},
        )
        
        return response

    def get_job(self, job_id: int) -> Any:
        """
        Gets the resource representation for a job in Airbyte.

        :param job_id: Required. Id of the Airbyte job
        """
        return self.run(
            endpoint=f"api/{self.connection.api_version}/jobs/get",
            json={"id": job_id},
            headers={"accept": "application/json"},
        )

    def cancel_job(self, job_id: int) -> Any:
        """
        Cancel the job when task is cancelled

        :param job_id: Required. Id of the Airbyte job
        """
        return self.run(
            endpoint=f"api/{self.connection.api_version}/jobs/cancel",
            json={"id": job_id},
            headers={"accept": "application/json"},
        )

    def get_latest_job(self, connection_id):
        """
        Gets the latest job for a connection in Airbyte.

        :param connection_id: Required. Id of the Airbyte connection

        # sample response
        {'job': {'id': 3, 'configType': 'sync', 'configId': 'c5bc8a5d-c9c4-4399-89f4-1d5dd34ef170', 
        'enabledStreams': [{'name': 'employee', 'namespace': 'public'}], 
        'createdAt': 1683785459, 'updatedAt': 1683785542, 'status': 'succeeded'}}
        """
        return self.run(
            endpoint=f"api/{self.connection.api_version}/jobs/get_last_replication_job",
            headers={"accept": "application/json"},
            json={"connectionId": connection_id},
            method="POST"
        )

    def list_source_definition(self):
        """Get list of source definitions

        Used to get definition id for creating source config
        """

        return self.run(
            endpoint=f"api/{self.connection.api_version}/v1/sources/list",
            headers={"accept": "application/json"},
            json={"workspaceId": workspaceId},
            method="POST"
        )

    def list_destination_definition(self):
        """Get list of destination definitions

        Used to get definition id for creating destination config
        """
        return self.run(
            endpoint=f"api/{self.connection.api_version}/v1/destination/list",
            headers={"accept": "application/json"},
            json={"workspaceId": workspaceId},
            method="POST"
        )

    def create_source(self):
        pass

    def create_destination(self):
        pass

    

    def test_connection(self):
        """Tests the Airbyte connection by hitting the health API
    
        """
        try:
            res = self.run(
                endpoint=f"api/{self.connection.api_version}/health",
                headers={"accept": "application/json", "Content-Type": "application/json;charset=utf-8"},
                extra_options={"check_response": False},
                method="GET"
            )

            if res.status_code == 200:
                return True, "Connection successfully tested"
            else:
                return False, res.text
        except Exception as e:
            return False, str(e)
