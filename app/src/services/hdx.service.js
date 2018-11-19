const logger = require('logger');
const requestPromise = require('request-promise');
const config = require('config');
const ctRegisterMicroservice = require('ct-register-microservice-node');

const ACCEPTED_LICENSE_STRINGS = [
    'Public Domain',
    'CC-0',
    'PDDL',
    'CC-BY',
    'CDLA-Permissive-1.0',
    'ODC-BY',
    'CC-BY-SA',
    'CDLA-Sharing-1.0',
    'ODC-ODbL',
    'CC BY-NC',
    'CC BY-ND',
    'CC BY-NC-SA',
    'CC BY-NC-ND',
    'Other'
];

class HDXIndexService {

    static async cronUpdate() {
        const timeout = ms => new Promise(res => setTimeout(res, ms))
        try {
            logger.info('Running cron update');
            logger.debug('Obtaining datasets');
            const datasets = await ctRegisterMicroservice.requestToMicroservice({
                method: 'GET',
                uri: `/dataset?provider=hdx&page[size]=99999`,
                json: true
            });
            if (datasets && datasets.data) {
                for (let i = 0, length = datasets.data.length; i < length; i++) {
                    try {
                        const dataset = datasets.data[i].attributes;
                        dataset.id = datasets.data[i].id;
                        await timeout(1000);
                        await HDXIndexService.register(dataset, dataset.userId, true);
                    } catch (err) {
                        logger.error('Error updating dataset', err);
                    }
                }
            }
        } catch (err) {
            logger.error('Error in cron update', err);
            throw err;
        }
    }

    static async register(dataset, userId, update = false) {
        logger.debug(`Obtaining metadata of HFX package ${dataset.tableName}`);

        logger.debug('Obtaining dataset info and metadata of HFX package ', `${config.hdx.package}`.replace(':package-id', dataset.tableName));
        let hdxPackage;

        try {
            const hdxPackageResponse = await requestPromise({
                method: 'GET',
                url: `${config.hdx.package}`.replace(':package-id', dataset.tableName),
                json: true
            });
            logger.debug('HDX package response', hdxPackageResponse);

            if (hdxPackageResponse && hdxPackageResponse.result && hdxPackageResponse.result.resources) {
                hdxPackage = hdxPackageResponse.result;
            } else {
                throw new Error(`Incomplete or invalid data loaded from HDX API: ${dataset.tableName}`);
            }

            if (hdxPackage.resources.length === 0) {
                throw new Error(`No resource data associated with this HDX package was found: ${dataset.tableName}`);
            }

            let hdxResource;
            const jsonResources = hdxPackage.resources.filter(elem => elem.format.toUpperCase() === 'JSON');

            if (jsonResources.length === 1) {
                hdxResource = jsonResources[0];
            } else {
                const csvResources = hdxPackageResponse.result.resources.filter(elem => elem.format.toUpperCase() === 'CSV');
                if (csvResources.length === 1) {
                    hdxResource = csvResources[0];
                }
            }

            if (!hdxResource) {
                throw new Error(`No single JSON or CSV resource found for HDX package ${dataset.tableName}`);
            }

            const dataDownloadURL = config.hdx.dataSourceEndpoint.replace(':resouce-file-path', hdxResource.hdx_rel_url);
            const dataSourceUrl = config.hdx.dataSourceUrl.replace(':package-id', dataset.tableName);

            const metadata = {
                language: 'en',
                name: hdxPackage.title || dataset.name,
                description: hdxResource.description,
                sourceOrganization: hdxPackage.organization.title,
                dataSourceUrl,
                dataSourceEndpoint: dataDownloadURL,
                dataDownloadUrl: dataDownloadURL,
                status: 'published',
                license: ACCEPTED_LICENSE_STRINGS.includes(hdxPackage.license) ? hdxPackage.license : 'Other',
                userId
            };
            logger.debug('Saving metadata', metadata);
            if (!update) {
                await ctRegisterMicroservice.requestToMicroservice({
                    method: 'POST',
                    uri: `/dataset/${dataset.id}/metadata`,
                    body: metadata,
                    json: true
                });
            } else {
                await ctRegisterMicroservice.requestToMicroservice({
                    method: 'PATCH',
                    uri: `/dataset/${dataset.id}/metadata`,
                    body: metadata,
                    json: true
                });
            }

        } catch (err) {
            logger.error('Error obtaining metadata', err);
            //update dataset to be published false;
            if(dataset) {
                let result = await requestPromise({
                    method: 'PATCH',
                    url: `https://api.apihighways.org/v1/dataset/${dataset.id}`,
                    body: {
                        published: false 
                    },
                    headers: {
                        Authorization: `Bearer ${process.env.CT_TOKEN}`,
                        "content-type": "application/json" 
                    },  
                    json: true
                });    
            }
            logger.debug(`setting dataset: ${dataset.id} publish to false ${result}`)
            logger.debug(`${result}`)

            throw new Error(`Error obtaining metadata: ${err}`);
        }

        if (!update && hdxPackage) {
            try {
                const body = {
                    legacy: {
                        tags: ['HDX API', hdxPackage.organization.title]
                    }
                };

                hdxPackage.tags.forEach(elem => body.legacy.tags.push(elem.name));

                logger.debug('Tagging dataset for HDX dataset', dataset.tableName);
                await ctRegisterMicroservice.requestToMicroservice({
                    method: 'POST',
                    uri: `/dataset/${dataset.id}/vocabulary`,
                    body,
                    json: true
                });
            } catch (err) {
                logger.error('Error tagging dataset', err);
                throw new Error(`Error tagging dataset: ${err}`);
            }
        }
    }

}

module.exports = HDXIndexService;
