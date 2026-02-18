"""
EC2 lifecycle: launch instance(s) via boto3, write instance config; terminate from config.
Requires: AWS credentials (env or ~/.aws/credentials).
Key pair name in AWS: default molecule-pkg-tests; override with EC2_KEY_NAME, MOLECULE_EC2_KEY_NAME, or ec2_key_name in platform config.
Private key path for SSH: SSH_KEY_PATH, MOLECULE_AWS_PRIVATE_KEY, or ~/.ssh/id_rsa.
Security group: when using a subnet and no security_group_ids are set, creates/uses a group named
  molecule-<vpc_id> with SSH (22), ICMP, and allow-all egress (same as playbooks/create.yml).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError
import yaml

logger = logging.getLogger(__name__)

DEFAULT_KEYPAIR_NAME = "molecule-pkg-tests"
SECURITY_GROUP_NAME_PREFIX = "molecule"
SECURITY_GROUP_DESCRIPTION = "Security group for testing Molecule"
# Wait timeouts (seconds); raise on timeout
DEFAULT_WAIT_TIMEOUT = 300
WAITER_POLL_INTERVAL = 15


def _waiter_config(timeout_seconds: int = DEFAULT_WAIT_TIMEOUT) -> dict[str, int]:
    """MaxAttempts so that Delay * MaxAttempts >= timeout_seconds. Raises WaiterError on timeout."""
    max_attempts = max(1, (timeout_seconds + WAITER_POLL_INTERVAL - 1) // WAITER_POLL_INTERVAL)
    return {"Delay": WAITER_POLL_INTERVAL, "MaxAttempts": max_attempts}


def _get_vpc_id_from_subnet(ec2_client: Any, subnet_id: str) -> str:
    """Return VPC ID for the given subnet."""
    out = ec2_client.describe_subnets(SubnetIds=[subnet_id])
    if not out.get("Subnets"):
        raise ValueError(f"Subnet not found: {subnet_id}")
    return out["Subnets"][0]["VpcId"]


def _get_or_create_molecule_security_group(
    ec2_client: Any,
    vpc_id: str,
    name_prefix: str = SECURITY_GROUP_NAME_PREFIX,
) -> str:
    """
    Get or create security group named <name_prefix>-<vpc_id> with playbook rules:
    ingress: TCP 22 (SSH), ICMP; egress: all. Returns group ID.
    """
    group_name = f"{name_prefix}-{vpc_id}"
    # See if it already exists
    try:
        desc = ec2_client.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": [group_name]},
            ]
        )
        if desc.get("SecurityGroups"):
            sg_id = desc["SecurityGroups"][0]["GroupId"]
            logger.info("Using existing security group: %s (%s)", group_name, sg_id)
            return sg_id
    except ClientError:
        pass

    try:
        create = ec2_client.create_security_group(
            GroupName=group_name,
            Description=SECURITY_GROUP_DESCRIPTION,
            VpcId=vpc_id,
        )
        sg_id = create["GroupId"]
        logger.info("Created security group: %s (%s)", group_name, sg_id)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "InvalidGroup.Duplicate":
            raise
        # Race: created between describe and create
        desc = ec2_client.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": [group_name]},
            ]
        )
        if not desc.get("SecurityGroups"):
            raise
        sg_id = desc["SecurityGroups"][0]["GroupId"]
        logger.info("Using existing security group (after duplicate): %s (%s)", group_name, sg_id)
        return sg_id

    # Ingress: SSH 22, ICMP, self (playbook security_group_rules)
    try:
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH"}],
                },
                {
                    "IpProtocol": "icmp",
                    "FromPort": 8,
                    "ToPort": -1,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "ICMP"}],
                },
                {
                    "IpProtocol": "-1",
                    "UserIdGroupPairs": [{"GroupId": sg_id}],
                },
            ],
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "InvalidPermission.Duplicate":
            raise
    # Egress: all (playbook security_group_rules_egress; default VPC SG may already have it)
    try:
        ec2_client.authorize_security_group_egress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "-1",
                    "FromPort": 0,
                    "ToPort": 0,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
            ],
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("InvalidPermission.Duplicate", "ResourceAlreadyExistsException"):
            raise
    return sg_id


def _get_key_name(platform_config: dict) -> str:
    """EC2 key pair name: env or platform override, else default molecule-pkg-tests."""
    name = (
        os.environ.get("EC2_KEY_NAME")
        or os.environ.get("MOLECULE_EC2_KEY_NAME")
        or platform_config.get("ec2_key_name")
    )
    return name if name else DEFAULT_KEYPAIR_NAME


def create_instances(
    platform_name: str,
    platform_config: dict[str, Any],
    key_path: str,
    out_config_path: str | Path,
    *,
    count: int = 1,
) -> list[dict]:
    """
    Launch EC2 instance(s), wait until running, write instance_config.yml.
    key_path: local path to the private key for SSH (key pair must exist in AWS).
    """
    out_config_path = Path(out_config_path)
    out_config_path.parent.mkdir(parents=True, exist_ok=True)

    region = platform_config.get("region", "us-west-2")
    image_id = platform_config.get("image")
    logger.info("Creating EC2 instance(s): platform=%s region=%s count=%s", platform_name, region, count)
    if not image_id:
        raise ValueError("platform config must have 'image' (AMI id)")
    instance_type = platform_config.get("instance_type", "t2.micro")
    subnet_id = platform_config.get("vpc_subnet_id")
    ssh_user = platform_config.get("ssh_user", "ec2-user")
    tags = platform_config.get("instance_tags") or {}
    # Security groups: explicit IDs from platform, or create/use molecule-<vpc_id> when subnet set (playbook behavior)
    security_group_ids = platform_config.get("security_group_ids")
    instance_profile_arn = (
        platform_config.get("instance_profile_arn")
        or os.environ.get("INSTANCE_PROFILE_ARN")
    )
    root_device_name = platform_config.get("root_device_name", "/dev/sda1")
    assign_public_ip = platform_config.get("assign_public_ip", True)

    key_name = _get_key_name(platform_config)
    key_path_resolved = Path(key_path).expanduser().resolve() if key_path else None
    logger.info("Key pair: name=%s private_key=%s", key_name, key_path_resolved)
    if not key_path_resolved or not key_path_resolved.exists():
        raise FileNotFoundError(
            f"SSH private key not found: {key_path_resolved or key_path}. "
            "Set SSH_KEY_PATH, MOLECULE_AWS_PRIVATE_KEY, or use ~/.ssh/id_rsa."
        )

    ec2 = boto3.client("ec2", region_name=region)

    # Resolve security group: use platform IDs or get/create molecule-<vpc_id> for subnet's VPC
    if security_group_ids is None and subnet_id:
        vpc_id = _get_vpc_id_from_subnet(ec2, subnet_id)
        sg_prefix = platform_config.get("security_group_name_prefix") or os.environ.get("SECURITY_GROUP_NAME_PREFIX", SECURITY_GROUP_NAME_PREFIX)
        sg_id = _get_or_create_molecule_security_group(ec2, vpc_id, name_prefix=sg_prefix)
        security_group_ids = [sg_id]
    if security_group_ids is not None and not isinstance(security_group_ids, list):
        security_group_ids = [security_group_ids]

    tag_specs = [
        {
            "ResourceType": "instance",
            "Tags": [{"Key": k, "Value": str(v)} for k, v in tags.items()]
            + [{"Key": "Name", "Value": f"pyinfra-{platform_name}"}, {"Key": "ssh_user", "Value": ssh_user}],
        }
    ]
    run_kwargs: dict[str, Any] = {
        "ImageId": image_id,
        "InstanceType": instance_type,
        "MinCount": count,
        "MaxCount": count,
        "KeyName": key_name,
        "TagSpecifications": tag_specs,
        # Same as playbooks/create.yml: root volume 40GB gp2, delete on termination
        "BlockDeviceMappings": [
            {
                "DeviceName": root_device_name,
                "Ebs": {
                    "VolumeSize": platform_config.get("root_volume_size", 40),
                    "VolumeType": platform_config.get("root_volume_type", "gp2"),
                    "DeleteOnTermination": True,
                },
            }
        ],
    }
    if instance_profile_arn:
        run_kwargs["IamInstanceProfile"] = {"Arn": instance_profile_arn}

    if subnet_id and (assign_public_ip or security_group_ids):
        ni: dict[str, Any] = {
            "SubnetId": subnet_id,
            "DeviceIndex": 0,
            "AssociatePublicIpAddress": assign_public_ip,
        }
        if security_group_ids:
            ni["Groups"] = security_group_ids
        run_kwargs["NetworkInterfaces"] = [ni]
    elif subnet_id:
        run_kwargs["SubnetId"] = subnet_id
        if security_group_ids:
            run_kwargs["SecurityGroupIds"] = security_group_ids

    logger.info(
        "Launching: image=%s type=%s subnet=%s assign_public_ip=%s security_groups=%s",
        image_id, instance_type, subnet_id, assign_public_ip, security_group_ids,
    )
    resp = ec2.run_instances(**run_kwargs)
    instance_ids = [inst["InstanceId"] for inst in resp["Instances"]]
    logger.info("Started instance(s): %s", ", ".join(instance_ids))

    # Wait for running (raise on timeout)
    wait_timeout = int(
        platform_config.get("wait_timeout")
        or os.environ.get("EC2_WAIT_TIMEOUT", DEFAULT_WAIT_TIMEOUT)
    )
    logger.info("Waiting for instance(s) to reach running state (timeout=%ds)...", wait_timeout)
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=instance_ids, WaiterConfig=_waiter_config(wait_timeout))

    # Fetch public/private IPs
    desc = ec2.describe_instances(InstanceIds=instance_ids)
    instance_configs = []
    for res in desc["Reservations"]:
        for inst in res["Instances"]:
            iid = inst["InstanceId"]
            # Prefer public IP for SSH; fall back to private (e.g. in same VPC)
            address = (
                inst.get("PublicIpAddress")
                or inst.get("PrivateIpAddress")
                or ""
            )
            if not address:
                raise RuntimeError(
                    f"Instance {iid} has no public or private IP (check subnet/public IP assignment)"
                )
            logger.info("Instance %s: address=%s user=%s", iid, address, ssh_user)
            instance_configs.append(
                {
                    "instance": inst.get("PrivateDnsName", iid),
                    "address": address,
                    "user": ssh_user,
                    "port": 22,
                    "identity_file": str(key_path_resolved) if key_path_resolved else "",
                    "instance_id": iid,
                    "region": region,
                }
            )

    with open(out_config_path, "w") as f:
        yaml.dump(instance_configs, f, default_flow_style=False, sort_keys=False)
    logger.info("Wrote instance config: %s (%d instance(s))", out_config_path, len(instance_configs))
    return instance_configs


def destroy_instances(config_path: str | Path, *, wait_timeout: int = DEFAULT_WAIT_TIMEOUT) -> None:
    """
    Fully destroy EC2 instance(s) (align with playbooks/destroy.yml):
    1. Read instance config from file (skip if missing/empty).
    2. Terminate instance(s) (ec2_instance state=absent equivalent).
    3. Wait for instance(s) deletion to complete (instance_terminated).
    4. Dump empty instance config to file.
    Raises on wait timeout (default 300s). Override via wait_timeout or EC2_WAIT_TIMEOUT env.
    """
    config_path = Path(config_path)
    logger.info("Destroy: populate instance config from %s", config_path)
    if not config_path.exists():
        logger.info("Destroy: config file missing, skip_instances=true")
        return
    try:
        with open(config_path) as f:
            instance_conf = yaml.safe_load(f) or []
    except Exception as e:
        logger.warning("Destroy: failed to read config (%s), skip_instances=true", e)
        return
    if not instance_conf:
        logger.info("Destroy: config empty, skip_instances=true")
        return

    instance_ids = [item["instance_id"] for item in instance_conf if item.get("instance_id")]
    if not instance_ids:
        with open(config_path, "w") as f:
            yaml.dump([], f)
        logger.info("Destroy: no instance_ids in config, cleared %s", config_path)
        return

    region = (instance_conf[0].get("region") or "us-west-2") if instance_conf else "us-west-2"
    logger.info("Destroy: region=%s instance_ids=%s", region, instance_ids)

    # Destroy molecule instance(s) (ec2_instance state=absent)
    timeout = int(os.environ.get("EC2_WAIT_TIMEOUT", wait_timeout))
    ec2 = boto3.client("ec2", region_name=region)
    ec2.terminate_instances(
        InstanceIds=instance_ids,
        SkipOsShutdown=True,
        Force=True,
    )
    logger.info("Destroy: requested termination of instance(s) %s (SkipOsShutdown=True, Force=True)", ", ".join(instance_ids))

    # Wait for instance(s) deletion to complete
    logger.info("Destroy: waiting for instance(s) deletion to complete (timeout=%ds)...", timeout)
    waiter = ec2.get_waiter("instance_terminated")
    waiter.wait(InstanceIds=instance_ids, WaiterConfig=_waiter_config(timeout))
    logger.info("Destroy: instance(s) deletion complete")

    # Dump instance config (empty, like playbook)
    with open(config_path, "w") as f:
        yaml.dump([], f)
    logger.info("Destroy: dumped empty instance config to %s", config_path)
