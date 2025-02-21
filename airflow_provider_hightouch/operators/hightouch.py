from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context

from airflow_provider_hightouch.hooks.hightouch import HightouchHook
from airflow_provider_hightouch.utils import parse_sync_run_details
from airflow_provider_hightouch.triggers.hightouch import HightouchTrigger


class HightouchLink(BaseOperatorLink):
    name = "Hightouch"

    def get_link(self, operator, dttm):
        return "https://app.hightouch.io"


class HightouchTriggerSyncOperator(BaseOperator):
    """
    This operator triggers a run for a specified Sync in Hightouch via the
    Hightouch API.

    :param sync_id: ID of the sync to trigger
    :type sync_id: int
    :param sync_slug: Slug of the sync to trigger
    :param connection_id: Name of the connection to use, defaults to hightouch_default
    :type connection_id: str
    :param api_version: Hightouch API version. Only v3 is supported.
    :type api_version: str
    :param synchronous: Whether to wait for the sync to complete before completing the task
    :type synchronous: bool
    :param deferrable: Whether to defer the execution of the operator
    :type deferrable: bool
    :param error_on_warning: Should sync warnings be treated as errors or ignored?
    :type error_on_warning: bool
    :param wait_seconds: Time to wait in between subsequent polls to the API.
    :type wait_seconds: float
    :param timeout: Maximum time to wait for a sync to complete before aborting
    :type timeout: int
    """

    operator_extra_links = (HightouchLink(),)

    @apply_defaults
    def __init__(
        self,
        sync_id: Optional[str] = None,
        sync_slug: Optional[str] = None,
        workspace_id: Optional[str] = None,
        connection_id: str = "hightouch_default",
        api_version: str = "v3",
        synchronous: bool = True,
        deferrable: bool = False,
        error_on_warning: bool = False,
        wait_seconds: float = 3,
        timeout: int = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hightouch_conn_id = connection_id
        self.api_version = api_version
        self.sync_id = sync_id
        self.sync_slug = sync_slug
        self.workspace_id = workspace_id
        self.error_on_warning = error_on_warning
        self.synchronous = synchronous
        self.deferrable = deferrable
        self.wait_seconds = wait_seconds
        self.timeout = timeout

    def execute(self, context: Context) -> str:
        """Start a Hightouch Sync Run"""
        hook = HightouchHook(
            hightouch_conn_id=self.hightouch_conn_id,
            api_version=self.api_version,
        )

        if not self.sync_id and not self.sync_slug:
            raise AirflowException(
                "One of sync_id or sync_slug must be provided to trigger a sync"
            )

        if self.deferrable:
            return self.handle_deferrable_execution(hook)

        if self.synchronous:
            self.log.info("Start synchronous request to run a sync.")
            hightouch_output = hook.sync_and_poll(
                self.sync_id,
                self.sync_slug,
                fail_on_warning=self.error_on_warning,
                poll_interval=self.wait_seconds,
                poll_timeout=self.timeout,
            )
            try:
                parsed_result = parse_sync_run_details(
                    hightouch_output.sync_run_details
                )
                self.log.info("Sync completed successfully")
                self.log.info(dict(parsed_result))
                return parsed_result.id
            except Exception:
                self.log.warning("Sync ran successfully but failed to parse output.")
                self.log.warning(hightouch_output)

        else:
            self.log.info("Start async request to run a sync.")
            request_id = hook.start_sync(self.sync_id, self.sync_slug)
            sync = self.sync_id or self.sync_slug
            self.log.info(
                "Successfully created request %s to start sync: %s", request_id, sync
            )
            return request_id

    def handle_deferrable_execution(self, hook: HightouchHook) -> str:
        """
        Handle the deferrable execution logic for triggering a Hightouch sync.

        This method checks whether to trigger a Hightouch sync using either a sync ID or a sync slug.
        It validates that exactly one of the two is provided and then initiates the sync asynchronously.
        The method defers execution until the sync completes, using a trigger to monitor the status.

        :param hook: An instance of HightouchHook used to interact with the Hightouch API.
        :type hook: HightouchHook

        :raises AirflowException: If neither sync_id nor sync_slug is provided, or if both are provided.

        :return: None
        """

        self.log.info("Using deferrable execution to trigger sync.")
        # Validate that exactly one of sync_id or sync_slug is provided
        if (self.sync_id is None) == (self.sync_slug is None):
            raise AirflowException(
                "Exactly one of sync_id or sync_slug must be provided."
            )

        if self.sync_slug:
            self.log.info(
                f"Triggering sync asynchronously using slug ID: {self.sync_slug}..."
            )
            sync_request_id = hook.start_sync(sync_slug=self.sync_slug)
            self.sync_id = hook.get_sync_from_slug(sync_slug=self.sync_slug)
        else:
            self.log.info(
                f"Triggering sync asynchronously using sync ID: {self.sync_id}..."
            )
            sync_request_id = hook.start_sync(sync_id=self.sync_id)

        self.log.info(
            f"Successfully started sync {self.sync_id}. Deferring execution..."
        )
        self.defer(
            trigger=HightouchTrigger(
                workspace_id=self.workspace_id,
                sync_id=self.sync_id,
                sync_request_id=sync_request_id,
                sync_slug=self.sync_slug,
                connection_id=self.hightouch_conn_id,
                poll_interval=self.wait_seconds,
                timeout=self.timeout,
            ),
            method_name=None,
        )
        return

    # def execute_complete(self, context: Context, event: dict):
    #     """
    #     Resumes execution after the trigger completes.

    #     This method is called after the Hightouch sync completes. It processes the
    #     event containing the sync status and raises exceptions if the sync fails or
    #     times out.

    #     Args:
    #         context (Context): The context in which the operator is executed.
    #         event (dict): The event containing the sync status.

    #     Raises:
    #         AirflowException: If the sync fails or times out.
    #     """
    #     status = event.get("status")
    #     message = event.get("message")

    #     if status == "success":
    #         self.log.info(message)
    #         return event.get("sync_id", None)
    #     elif status in ["failed", "timeout"]:
    #         raise AirflowException(message)
    #     else:
    #         raise AirflowException(message)
