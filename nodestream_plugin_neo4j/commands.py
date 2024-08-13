import os
from time import sleep

from cleo.helpers import argument, option
from neo4j_aura_sdk import AuraClient, models
from nodestream.cli.commands import NodestreamCommand

AURA_CLIENT_SECRET_ENV = "AURA_CLIENT_SECRET"
AURA_CLIENT_SECRET_OPT = "aura-client-secret"
AURA_CLIENT_ID_ENV = "AURA_CLIENT_ID"
AURA_CLIENT_ID_OPT = "aura-client-id"


class CreateAura(NodestreamCommand):
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
    options = [
        option(AURA_CLIENT_ID_OPT, description="Aura API client id", flag=False),
        option(
            AURA_CLIENT_SECRET_OPT, description="Aura API client secret", flag=False
        ),
        option("wait", "w", "Wait for instance status to be running"),
    ]

    async def handle_async(self):
        # credentials
        aura_client_id, aura_client_secret = get_aura_credentials(self)
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

            if self.option("wait"):
                self.line(f"\nCreating instance...")
                if resp.data.status != "running":
                    while resp.data.status != "running":
                        sleep(10)
                        resp = await client.instance(instance_id)
                if resp.data.status != "running":
                    self.line_error(
                        f'<error>Error creating instance "{instance_id}" with status "{resp.data.status}"</error>'
                    )
                    return
                self.line("Instance is now running")
            else:
                self.line(
                    f"\nInstance is being created. To check on its status, run:\n   "
                    + f"nodestream status aura {instance_id}"
                )


class StatusAura(NodestreamCommand):
    name = "aura status"
    description = "Check status of Aura instance via the Aura API"
    arguments = [
        argument("instance_id", "instance id"),
    ]
    options = [
        option(AURA_CLIENT_ID_OPT, description="Aura API client id", flag=False),
        option(
            AURA_CLIENT_SECRET_OPT, description="Aura API client secret", flag=False
        ),
    ]

    async def handle_async(self):
        # credentials
        aura_client_id, aura_client_secret = get_aura_credentials(self)
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
            resp = await client.instance(self.argument("instance_id"))
            self.line(f"Instance Name: {resp.data.name}")
            self.line(f"Status: {resp.data.status}")


class RemoveAura(NodestreamCommand):
    name = "aura remove"
    description = "Remove Aura instance by instance Id"
    arguments = [
        argument("instance_id", "instance id"),
    ]
    options = [
        option(AURA_CLIENT_ID_OPT, description="Aura API client id", flag=False),
        option(
            AURA_CLIENT_SECRET_OPT, description="Aura API client secret", flag=False
        ),
    ]

    async def handle_async(self):
        # credentials
        aura_client_id, aura_client_secret = get_aura_credentials(self)
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
            resp = await client.delete_instance(self.argument("instance_id"))
            self.line(f"{resp}")


def get_aura_credentials(ctx: NodestreamCommand):
    client_id = ctx.option(AURA_CLIENT_ID_OPT)
    if not client_id:
        client_id = os.environ.get(AURA_CLIENT_ID_ENV, "")
    client_secret = ctx.option(AURA_CLIENT_SECRET_OPT)
    if not client_secret:
        client_secret = os.environ.get(AURA_CLIENT_SECRET_ENV, "")
    return client_id, client_secret
