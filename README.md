# db_relocate

The purpose of this tool is to simplify and automate database migration or upgrade process without downtime.

## How to run
`./db_relocate run`

## What exactly does this tool do?
1. Run pre-flight checks.
2. Start background health check process.
3. Create publication and replication slot.
4. Take a snapshot of the source database.
5. Upgrade the snapshot to the desired engine version (apply encryption if none).
6. Restore a new database instance from the snapshot.
7. Create a subscription on the destination database.
8. Advance replication to the correct LSN at the moment when the snapshot was taken.
9. Sync the data that was changed after the snapshot was taken.
10. Verify that all the heartbeat records have been synced.
11. Update sequences by leaving a small gap to avoid any conflicts.

After all the above steps have been processed, you will have two RDS PostgreSQL databases fully synced without any data loss. It is also your responsibility to double-check that all the data has been synced after the snapshot has been taken. The health check process will help you to be more confident.

TODO: It will be automated soon as well.
As a final step, you need to deploy a load balancer (e.g., HAProxy), point your application to it, and then switch the traffic straight away. Then, you need to do a cleanup and point your app directly to the new database.

The entire procedure takes about an hour to run.

## Configuration

The db_relocate accepts a YAML configuration file, the location of which can be specified by the `-config` flag.

Example configuration file:
```yaml
---
aws:
 profile: "profile-name"
 region: "eu-west-1"
src:
 user: "user"
 password: "password"
 name: "mydatabase"
 host: "test-db.someid.eu-west-1.rds.amazonaws.com"
 instance_id: "test-db"
upgrade:
 subnet_group: "test-db-subnet"
 engine_version: "13.7" # target db version
 parameter_group: "test-dn"
 password: "upgradeUser" # required for replication
```

### Top level options

Name              | Description
------------------|------------
`src`             | Source database configuration block.
`dst`             | Destination database configuration block.
`upgrade`         | Upgrade details configuration block.
`aws`             | AWS-related configuration block.
`force`           | (default: false) A boolean value to indicate whether to proceed with the upgrade process forcefully.
`log_level`       | (default: info) The minimum level of log messages to display. Possible values are debug, info, warn, error, and fatal.

### AWS configuration block options
Name              | Description
------------------|------------
`profile`         | (default: default) The AWS profile name to use when connecting to the AWS services.
`region`          | (default: us-east-1) The AWS region to use when connecting to the AWS services.

### Source database configuration block options
Name              | Description
------------------|------------
`user`            | (default: ops) The username to use when connecting to the source database.
`password`        | (default: secret) The password to use when connecting to the source database.
`schema`          | (default: public) The schema to use when connecting to the source database.
`name`            | (default: postgres) The name of the source database.
`port`            | (default: 5432) The port number of the source database.
`host`            | (default: 127.0.0.1) The hostname or IP address of the source database.
`instance_id`     | (default: "") The instance identifier of the source database.

### Destination database configuration block options
Name              | Description
------------------|------------
`user`            | (default: ops) The username to use when connecting to the destination database. Not required because the value will be copied from the source database configuration section.
`password`        | (default: secret) The password to use when connecting to the destination database. Not required because the value will be copied from the source database configuration section.
`schema`          | (default: public) The schema to use when connecting to the destination database. Not required because the value will be copied from the source database configuration section.
`name`            | (default: postgres) The name of the destination database. Not required because the value will be copied from the source database configuration section.
`port`            | (default: 5432) The port number of the destination database. Not required because the value will be copied from the source database configuration section.
`host`            | (default: 127.0.0.1) The hostname or IP address of the destination database. Not required.
`instance_id`     | (default: "") The instance identifier of the destination database. If not specified will be auto-generated.

### Upgrade configuration block options
Name                | Description
--------------------|------------
`subnet_group`      | (default: "") The name of the DB subnet group to use for the new instance. If not specified will be copied from the srouce database.
`engine_version`    | (default: "") The version of the database engine to upgrade to. Must be higher than used by a source database.
`kms_id`            | (default: "") The ID of the KMS key to use for encrypting the new instance. KMS key id to use in case source database is not encrypted. If not provided default one will be used.
`security_groups`   | (default: list) A list of security group IDs to use for the new instance. If not provided will be copied from the source database.
`parameter_group`   | (default: "") The name of the DB parameter group to use for the new instance. Must be compatible with engine version you are upgrading to.
`instance_class`    | (default: "") The instance class of the new instance.. If not provided will be copied from the source database.
`storage_type`      | (default: "") The storage type of the new instance. If not provided will be copied from the source database.
`user`              | (default: upgrade) The username to use when creating a user for logical replication.
`password`          | (default: s4p3rs3cr3t!) The password to use when creating a user for logical replication.
`vpc_id`            | (default: "") The ID of the VPC to use during pre-flight checks(e.g: security groups, subnet_group). If not provided will be copied from the source database.


## Future plans
Time            |   Goal
----------------|-------
`short-term`    | 1. Increase test coverage. 2. Provision load balancer in the end automatically. 3. Spin-up read replicas(if any).
`mid-term`      | 1. Add MySQL as well. 2. Add google cloud resources potentially.
`long-term`     | 1. Create a kubernetes operator which will perform upgrade/migration routine completely automatically without downtime.
