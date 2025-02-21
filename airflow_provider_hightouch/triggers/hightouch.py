import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import (
    BaseTrigger,
    TriggerEvent,
    TaskSuccessEvent,
    TaskFailedEvent,
)
from airflow_provider_hightouch.hooks.hightouch import HightouchAsyncHook


class HightouchTrigger(BaseTrigger):
    """
    Trigger to monitor a Hightouch sync run asynchronously.

    This trigger checks the status of a Hightouch sync run at regular intervals
    until it completes, fails, or times out. It uses the Hightouch API to retrieve
    the sync status and yields events based on the sync's progress.

    Args:
        sync_run_url (str): Constructed sync_run_url.
        sync_id (str): The ID of the Hightouch sync.
        sync_request_id (str): The request ID of the sync run to monitor.
        sync_slug (str): The slug of the Hightouch sync.
        connection_id (str): The Airflow connection ID for Hightouch API access.
        timeout (float): The maximum time (in seconds) to wait before timing out.
        poll_interval (float): The time (in seconds) to wait between status checks.
    """

    def __init__(
        self,
        sync_run_url: Optional[str],
        sync_id: Optional[str],
        sync_request_id: str,
        sync_slug: Optional[str],
        connection_id: str,
        timeout: float,
        end_from_trigger: bool = True,
        poll_interval: float = 4.0,
    ) -> None:
        """
        Initializes the HightouchTrigger with the provided parameters.

        Args:
            sync_run_url (str): Constructed sync_run_url.
            sync_id (str): The ID of the Hightouch sync.
            sync_request_id (str): The request ID of the sync run to monitor.
            sync_slug (str): The slug of the Hightouch sync.
            connection_id (str): The Airflow connection ID for Hightouch API access.
            timeout (float): The maximum time (in seconds) to wait before timing out.
            end_from_trigger (bool): Allows for task to complete from the trigger. Default is true.
            poll_interval (float): The time (in seconds) to wait between status checks.
        """
        super().__init__()
        self.sync_run_url = sync_run_url
        self.sync_id = sync_id
        self.sync_request_id = sync_request_id
        self.sync_slug = sync_slug
        self.connection_id = connection_id
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.end_from_trigger = end_from_trigger
        self.hook = HightouchAsyncHook(hightouch_conn_id=self.connection_id)

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serialize the trigger state for storage.

        Returns:
            Tuple[str, Dict[str, Any]]: A tuple containing the fully qualified class name
            and a dictionary of the trigger's parameters for state restoration.
        """
        return (
            f"{self.__module__}.{self.__class__.__name__}",
            {
                "sync_run_url": self.sync_run_url,
                "sync_id": self.sync_id,
                "sync_request_id": self.sync_request_id,
                "sync_slug": self.sync_slug,
                "connection_id": self.connection_id,
                "timeout": self.timeout,
                "end_from_trigger": self.end_from_trigger,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Periodically checks the sync status until completion or timeout.

        This method uses the Hightouch API to check the status of a sync run at
        regular intervals defined by the poll_interval. It yields TriggerEvents
        based on the current status of the sync.

        Yields:
            AsyncIterator[TriggerEvent]: Events indicating the status of the sync run,
            which can be "success", "failed", "timeout", or the current status during polling.
        """
        start_time = asyncio.get_event_loop().time()

        while True:

            try:
                # Fetch the current sync status
                response = await self.hook.get_sync_run_details(
                    self.sync_id, self.sync_request_id
                )

                status = response[0].get("status")

                # Handle different sync statuses
                if status in ["success", "completed"]:
                    self.log.info(f"{self.sync_run_url} finished with status {status}!")
                    yield TaskSuccessEvent()
                    return

                elif status in ["queued", "processing", "querying"]:
                    self.log.info(
                        f"Sync is {status}... Sleeping for {self.poll_interval} seconds."
                    )
                    await asyncio.sleep(self.poll_interval)

                elif status in ["failed", "error"]:
                    self.log.info(
                        f"{self.sync_run_url} finished with status {status}!\n"
                        f"Sync Error: {response[0]['error']}"
                    )
                    yield TaskFailedEvent()
                    return

                # Check for timeout
                if asyncio.get_event_loop().time() - start_time > self.timeout:
                    self.log.info(
                        f"{self.sync_run_url} exceeded DAG timeout of {self.timeout} seconds."
                    )
                    yield TaskFailedEvent()

            except Exception as e:
                self.log.error("Error while checking sync status: %s", str(e))
                yield TaskFailedEvent()
                return
