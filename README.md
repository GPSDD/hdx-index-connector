# HDX Index Adapter


This repository is the microservice that implements the HDX Index Adapter functionality.

1. [Getting Started](#getting-started)

## Getting Started

### OS X

**First, make sure that you have the [API gateway running
locally](https://github.com/control-tower/control-tower).**

We're using Docker which, luckily for you, means that getting the
application running locally should be fairly painless. First, make sure
that you have [Docker Compose](https://docs.docker.com/compose/install/)
installed on your machine.

```
git clone https://github.com/GPSDD/hdx-index-connector.git
cd hdx-index-connector
./adapter.sh develop
```text

You can now access the microservice through the CT gateway.

```

### Configuration

It is necessary to define these environment variables:

* CT_URL => Control Tower URL
* NODE_ENV => Environment (prod, staging, dev)

### Cron task

This component executes a periodic task that updates the metadata of each indexed RW dataset. The task is bootstrapped  
[when the application server starts](https://github.com/GPSDD/hdx-index-connector/blob/master/app/src/app.js#L19). 
The task's implementation can be found on `app/src/cron/cron` and the configuration is loaded from the 
[config files](https://github.com/GPSDD/hdx-index-connector/blob/master/config/default.json#L18)


## Field correspondence

The field correspondence is based on the metadata object for a single package - I.E. [this link](https://data.humdata.org/api/3/action/package_show?id=141121-sierra-leone-health-facilities).
In the HDX domain, a `package` entity may refer to multiple data files - identified as `resources`. 
Given that this structure does not match directly to the API structure, we use the following logic to map the HDX domain structure to ours:

1. Each HDX `package` tentatively corresponds to one API Highways dataset.
2. Within each `package`, if there's one and only `resource` with `format` of type `JSON`, we use that `resource` on step 4. If not, we proceed to step 3.
3. Within each `package`, if there's one and only `resource` with `format` of type `CSV`, we use that `resource` on step 4. If not, the `dataset` status is set to `failed` and no metadata is created.
4. We combine the `package` data and the selected `resource` data to generate metadata as described in the spec table below. 

| Field in SDG Metadata     | Field in HDX data     | Value         |
|---------------------------|-----------------------|---------------|
| userId                    |                       |               |
| language                  |                       | 'en'          |
| resource                  |                       |               |
| name                      | `package.title`, or the name provided on the create request as fallback |               |
| description               | `package.resource.description`        | |
| sourceOrganization        | `package.organization.title`           | |
| dataDownloadUrl           | 'https://data.humdata.org' + `package.resource.hdx_rel_url` | |
| dataSourceUrl             | 'https://data.humdata.org/dataset/' + `package.name`        | |
| dataSourceEndpoint        | 'https://data.humdata.org' + `package.resource.hdx_rel_url` | |
| license                   | Try to match the value of `package.license` to one of the [accepted licenses](https://data.world/license-help), fallback to 'Other'  | |
| status                    |                       | 'published'   |


## Dataset tagging strategy


### Taxonomy

HDX datasets have tags associated with them, which this connector uses to tag the index datasets. The tags are
 loaded from the HDX metadata response using the following JSONPath expression: `$.result.tags[*].display_name`
Additionally, each HDX dataset is tagged with the "HDX API" tag, and a tag to match 
[the RW API application to which they belong](https://github.com/GPSDD/resource-watch-index-adapter/blob/b5c32ee91018b60df56048b8df2b303787796bba/app/src/services/resourcewatch.service.js#L101).

### Graph

No graph tagging is done on HDX datasets.
