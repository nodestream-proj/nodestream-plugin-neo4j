import os
from time import sleep

from cleo.helpers import argument, option
from neo4j_aura_sdk import AuraClient, models
from nodestream.cli.commands import NodestreamCommand

AURA_CLIENT_SECRET_ENV = "AURA_CLIENT_SECRET"
AURA_CLIENT_SECRET_OPT = "aura-client-secret"
AURA_CLIENT_ID_ENV = "AURA_CLIENT_ID"
AURA_CLIENT_ID_OPT = "aura-client-id"

INSTANCE_ID_ARGUMENT = argument("instance_id", "instance id")
AURA_CLIENT_ID_OPTION = option(
    AURA_CLIENT_ID_OPT, description="Aura API client id", flag=False
)
AURA_CLIENT_SECRET_OPTION = option(
    AURA_CLIENT_SECRET_OPT, description="Aura API client secret", flag=False
)

RUNNING_STATUS = "running"
CREATING_STATUS = "creating"
DESTROYING_STATUS = "destroying"


class AuraCommand(NodestreamCommand):
    """Wrapper class around NodestreamCommand to handle Aura credential validation"""

    name = "base name"
    description = "base descrip"
    options = [AURA_CLIENT_ID_OPTION, AURA_CLIENT_SECRET_OPTION]

    async def handle_async(self):
        # credentials
        aura_client_id = self.option(AURA_CLIENT_ID_OPT)
        if not aura_client_id:
            aura_client_id = os.environ.get(AURA_CLIENT_ID_ENV, "")
        aura_client_secret = self.option(AURA_CLIENT_SECRET_OPT)
        if not aura_client_secret:
            aura_client_secret = os.environ.get(AURA_CLIENT_SECRET_ENV, "")

        if not aura_client_id:
            self.line_error(
                f"<error>{AURA_CLIENT_ID_ENV} variable required or provide --{AURA_CLIENT_ID_OPT}</error>"
            )
            return

        if not aura_client_secret:
            self.line_error(
                f"<error>{AURA_CLIENT_SECRET_ENV} variable required or provide --{AURA_CLIENT_SECRET_OPT}</error>"
            )
            return

        async with AuraClient(aura_client_id, aura_client_secret) as client:
            await self.interact_with_aura(client)

    async def interact_with_aura(self, client: AuraClient):
        raise NotImplementedError


class CreateAura(AuraCommand):
    name = "aura create"
    description = "Create neo4j Aura instances via the Aura API"
    arguments = [
        argument("name", "instance name"),
        argument("region", "region to create new aura instance"),
        argument(
            "instance_type",
            "instance type - possible values: professional-db, professional-ds, enterprise-db, enterprise-ds",
        ),
        argument("memory", "GB RAM for instance"),
        argument(
            "cloud_provider",
            "cloud provider the instance will be hosted in - possible values: gcp, aws",
        ),
        argument("tenant_id", "tenant_id for the new aura instance"),
    ]
    options = AuraCommand.options + [
        option("wait", "w", "Wait for instance status to be running")
    ]

    async def interact_with_aura(self, client: AuraClient):
        resp = await client.create_instance(
            models.InstanceRequest(
                name=self.argument("name"),
                memory=self.argument("memory"),
                version="5",
                region=self.argument("region"),
                cloud_provider=self.argument("cloud_provider"),
                tenant_id=self.argument("tenant_id"),
                type=self.argument("instance_type"),
            )
        )

        d = resp.data
        instance_id = d.id
        self.line("\nAURA INSTANCE DETAILS:\n")
        self.line(f"Instance Id = {instance_id}")
        self.line(f"Instance name = {d.name}")
        self.line(f"cloud_provider = {d.cloud_provider}")
        self.line(f"region = {d.region}")
        self.line(f"tenant_id = {d.tenant_id}")
        self.line(f"type = {d.type}")
        self.line(f"connection_url = {d.connection_url}")
        self.line(f"username = {d.username}")
        self.line(f"password = {d.password}")
        self.line("")

        if self.option("wait"):
            with self.spin("Waiting for instance to be ready...", ""):
                # note: resp.data.status is None from the initial create_instance call above
                while True:
                    sleep(10)
                    resp = await client.instance(instance_id)
                    if resp.data.status != CREATING_STATUS:
                        break

            # error check
            if resp.data.status != RUNNING_STATUS:
                self.line_error(
                    f'<error>Error creating instance "{instance_id}" with status "{resp.data.status}"</error>'
                )
                return
            self.line("Instance is now running")
        else:
            self.line(
                "\nInstance is being created. To check on its status, run:\n   "
                + f"nodestream status aura {instance_id}"
            )


class StatusAura(AuraCommand):
    name = "aura status"
    description = "Check status of Aura instance via the Aura API"
    arguments = [INSTANCE_ID_ARGUMENT]

    async def interact_with_aura(self, client: AuraClient):
        with self.spin("Checking status...", ""):
            resp = await client.instance(self.argument("instance_id"))
        self.line(f"Instance Name: {resp.data.name}")
        self.line(f"Status: {resp.data.status}")


class RemoveAura(AuraCommand):
    name = "aura remove"
    description = "Remove Aura instance by instance Id"
    arguments = [INSTANCE_ID_ARGUMENT]

    async def interact_with_aura(self, client: AuraClient):
        with self.spin("Removing instance...", ""):
            resp = await client.delete_instance(self.argument("instance_id"))
            self.line(f"{resp}")
