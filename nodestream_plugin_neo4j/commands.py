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
            self.line_error(
                '<error>The "--aura_client_secret" option is required</error>'
            )
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

            instance_id = resp.data.id
            username = resp.data.username
            password = resp.data.password
            self.line(f"Instance Id = {instance_id}")
            self.line(f"username = {username}")
            self.line(f"password = {password}")
            self.line('Waiting for "running" status...')
            if resp.data.status != "running":
                while resp.data.status != "running":
                    sleep(10)
                    resp = await client.instance(instance_id)
            if resp.data.status != "running":
                self.line_error(
                    f'<error>Error creating instance "{instance_id}" with status "{resp.data.status}"</error>'
                )
                return
            self.line("Created")


class StatusAura(NodestreamCommand):
    name = "status aura"
    description = "Check status of Aura instance via the Aura API"
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
            self.line_error(
                '<error>The "--aura_client_secret" option is required</error>'
            )
            return

        aura_client_id = self.argument("aura_client_id")
        async with AuraClient(aura_client_id, aura_client_secret) as client:
            resp = await client.instance(self.argument("instance_id"))
            self.line(f"status={resp.data.status}")


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
            self.line_error(
                '<error>The "--aura_client_secret" option is required</error>'
            )
            return

        async with AuraClient(
            self.argument("aura_client_id"), aura_client_secret
        ) as client:
            await client.delete_instance(self.argument("instance_id"))
