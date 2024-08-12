from time import sleep

from cleo.helpers import argument, option
from neo4j_aura_sdk import AuraClient, models
from nodestream.cli.commands import NodestreamCommand


class CreateAura(NodestreamCommand):
    name = "create aura"
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
        argument("aura_client_id", "aura api client id"),
    ]
    options = [
        # making this an option because the secret can start with a dash/hyphen, and
        # get interpreted as another option
        option("aura_client_secret", "aura api client secret", flag=False)
    ]

    async def handle_async(self):
        aura_client_secret = self.option("aura_client_secret")
        if not aura_client_secret:
            self.line_error("<error>--aura_client_secret is required</error>")
            return

        name = self.argument("name")
        self.line(f"Creating instance '{name}'...")
        aura_client_id = self.argument("aura_client_id")
        async with AuraClient(aura_client_id, aura_client_secret) as client:
            resp = await client.create_instance(
                models.InstanceRequest(
                    name=name,
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
            self.line("")
            self.line(f"Instance Id = {instance_id}")
            self.line(f"Instance name = {d.name}")
            self.line(f"cloud_provider = {d.cloud_provider}")
            self.line(f"region = {d.region}")
            self.line(f"tenant_id = {d.tenant_id}")
            self.line(f"type = {d.type}")
            self.line(f"connection_url = {d.connection_url}")
            self.line(f"username = {d.username}")
            self.line(f"password = {d.password}")
            self.line(
                f"\nInstance created. To check on status of instance, run:\n   "
                + f"nodestream status aura {instance_id} $AURA_CLIENT_ID --aura_client_secret=$AURA_CLIENT_SECRET"
            )


class StatusAura(NodestreamCommand):
    name = "status aura"
    description = "Check status of Aura instance via the Aura API"
    arguments = [
        argument("instance_id", "instance id"),
        argument("aura_client_id", "Aura API client id"),
    ]
    options = [option("aura_client_secret", "Aura API client secret", flag=False)]

    async def handle_async(self):
        aura_client_secret = self.option("aura_client_secret")
        if not aura_client_secret:
            self.line_error("<error>--aura_client_secret is required</error>")
            return

        aura_client_id = self.argument("aura_client_id")
        async with AuraClient(aura_client_id, aura_client_secret) as client:
            resp = await client.instance(self.argument("instance_id"))
            self.line(f"Instance Name: {resp.data.name}")
            self.line(f"Status: {resp.data.status}")


class RemoveAura(NodestreamCommand):
    name = "remove aura"
    description = "Remove Aura instance by instance Id"
    arguments = [
        argument("instance_id", "instance id"),
        argument("aura_client_id", "aura api client id"),
    ]
    options = [
        # making this an option because the secret can being with a dash/hyphen, which
        # gets interpreted as another option
        option("aura_client_secret", "aura api client secret", flag=False)
    ]

    async def handle_async(self):
        aura_client_secret = self.option("aura_client_secret")
        if not aura_client_secret:
            self.line_error("<error>--aura_client_secret is required</error>")
            return

        async with AuraClient(
            self.argument("aura_client_id"), aura_client_secret
        ) as client:
            resp = await client.delete_instance(self.argument("instance_id"))
            self.line(f"{resp}")
