import uuid
import typer
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from skyplane.api.config import TransferConfig
from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig, IBMCloudConfig, SCPConfig
from skyplane.config_paths import cloud_config
from skyplane.api.provisioner import Provisioner
from skyplane.api.obj_store import ObjectStore
from skyplane.api.usage import get_clientid
from skyplane.utils import logger
from skyplane.utils.definitions import tmp_log_dir

from skyplane.api.pipeline import Pipeline

if TYPE_CHECKING:
    from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig, TransferConfig, IBMCloudConfig, SCPConfig


class SkyplaneClient:
    """Client for initializing cloud provider configurations."""

    def __init__(
        self,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        ibmcloud_config: Optional["IBMCloudConfig"] = None,
        scp_config: Optional["SCPConfig"] = None,
        transfer_config: Optional["TransferConfig"] = None,
        log_dir: Optional[str] = None,
    ):
        """
        :param aws_config: aws cloud configurations
        :type aws_config: class AWSConfig (optional)
        :param azure_config: azure cloud configurations
        :type azure_config: class AzureConfig (optional)
        :param gcp_config: gcp cloud configurations
        :type gcp_config: class GCPConfig (optional)
        :param ibmcloud_config: ibm cloud configurations
        :type ibmcloud_config: class IBMCloudConfig (optional)
        :param transfer_config: transfer configurations
        :type transfer_config: class TransferConfig (optional)
        :param log_dir: path to store transfer logs
        :type log_dir: str (optional)
        """
        self.clientid = get_clientid()

        # If a specific config is not passed in, create a default one based on the global config file.
        # This is the key change: it ensures the enabled/disabled status is always loaded correctly.
        if aws_config is None:
            aws_config = AWSConfig(aws_enabled=cloud_config.aws_enabled)
        if azure_config is None and cloud_config.azure_enabled:
            azure_config = AzureConfig(
                azure_subscription_id=cloud_config.azure_subscription_id,
                azure_resource_group=cloud_config.azure_resource_group,
                azure_umi_id=cloud_config.azure_principal_id,
                azure_umi_name=cloud_config.azure_umi_name,
                azure_umi_client_id=cloud_config.azure_client_id,
                azure_enabled=True
            )
        if gcp_config is None:
            gcp_config = GCPConfig(gcp_project_id=cloud_config.gcp_project_id, gcp_enabled=cloud_config.gcp_enabled)
        if ibmcloud_config is None:
            ibmcloud_config = IBMCloudConfig(ibmcloud_enabled=cloud_config.ibmcloud_enabled)
        if scp_config is None:
            scp_config = SCPConfig(scp_enabled=cloud_config.scp_enabled)
        
        # Based on the configs, create the auth providers. This will be None if the provider is disabled.
        self.aws_auth = aws_config.make_auth_provider() if aws_config and aws_config.aws_enabled else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config and azure_config.azure_enabled else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config and gcp_config.gcp_enabled else None
        self.ibmcloud_auth = ibmcloud_config.make_auth_provider() if ibmcloud_config and ibmcloud_config.ibmcloud_enabled else None
        self.scp_auth = scp_config.make_auth_provider() if scp_config and scp_config.scp_enabled else None

        self.transfer_config = transfer_config if transfer_config else TransferConfig()
        self.log_dir = (
            tmp_log_dir / "transfer_logs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}"
            if log_dir is None
            else Path(log_dir)
        )

        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.open_log_file(self.log_dir / "client.log")
        typer.secho(f"Logging to: {self.log_dir / 'client.log'}", fg="bright_black")

        # The Provisioner will now receive `None` for any disabled providers, preventing the crash.
        self.provisioner = Provisioner(
            host_uuid=self.clientid,
            aws_auth=self.aws_auth,
            azure_auth=self.azure_auth,
            gcp_auth=self.gcp_auth,
            ibmcloud_auth=self.ibmcloud_auth,
            scp_auth=self.scp_auth,
        )

    def pipeline(self, planning_algorithm: Optional[str] = "direct", max_instances: Optional[int] = 1, debug=False):
        """Create a pipeline object to queue jobs"""
        return Pipeline(
            planning_algorithm=planning_algorithm,
            max_instances=max_instances,
            clientid=self.clientid,
            provisioner=self.provisioner,
            transfer_config=self.transfer_config,
            debug=debug,
        )

    def copy(self, src: str, dst: str, recursive: bool = False):
        """
        A simple version of Skyplane copy. It automatically waits for transfer to complete
        (the main thread is blocked) and deprovisions VMs at the end.

        :param src: Source prefix to copy from
        :type src: str
        :param dst: The destination of the transfer
        :type dst: str
        :param recursive: If true, will copy objects at folder prefix recursively (default: False)
        :type recursive: bool
        :param num_vms: The maximum number of instances to use per region (default: 1)
        :type num_vms: int
        """

        pipeline = self.pipeline()
        pipeline.queue_copy(src, dst, recursive=recursive)
        pipeline.start(progress=True)

    def object_store(self):
        return ObjectStore()
