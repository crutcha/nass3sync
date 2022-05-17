# Warden

Warden is an agent for keeping your local filesystem in sync with remote object storage. Currently, it can be used to create tarball backups of specified directories and monitor specific paths and keep them in sync with object storage. It was originally developed to work with S3 or GCS but can be easily extended to other object stores.

### Features

* **Backups:** Backup specified paths to buckets. Backup names will be determined based on path and timestamp (IE: /var/lib/myapp would be `var_lib_myapp_<date>.tar.gz`)
* **Sync(non-destructive):** Crawl specific paths and upload new/updated files, files deleted on local filesystem will not be deleted from buckets.
* **Sync(destructive):** Crawl specific paths and upload new/updated files, files deleted on local filesystem will be copied from the sync backup to a tombstone bucket, then deleted from the sync bucket.
* **Notifications:** Notifications will be sent upon every sync/backup job. Currently, only SNS is supposed, but this can easily be extended to support something else (sendgrid, etc).
* **Exclusion Patterns:** Files can be excluded from sync via regex patterns

## Install

TODO

## Configuration
Configuration is done via a YAML file. The default location for the config file is /etc/warden.yml but can be overriden using the `-configfile` cli flag.
```
warden -configfile myconfig.yml
```
Example configuration with comments:
```
provider:
  # either aws or gcp
  name: aws
  # IAM profile for AWS, not required for GCP
  profile: nass3sync
  # AWS or GCP region
  region: us-east-2
  # credential file path for GCP auth, not required for AWS
  credentialfile: "/home/me/gcpauth.json"

# Defines how many uploads will be done in parralel across all currently running sync/backup jobs
concurrency: 5
# SNS config
notify:
    service: sns
    id: arn:aws:sns:us-east-2:01234567890:somesnstopic

# list of paths to sync
sync:
  - sourcefolder: /home/me/somedatadirectory
    destinationbucket: my-sync-bucket
    # interval time in minutes to execute sync
    interval: 60
    exclude:
      - ".*/myappdata/notthisfoldertho/.*"

# list of paths to backup
backup:
  - sourcefolder: /home/me/someotherdatadir
    destinationbucket: my-backup-bucket
    # crontab syntax for when to execute backups for this path. in this case, everyday at midnight
    at: "0 0 */1 * *"
```

