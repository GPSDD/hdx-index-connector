const logger = require('logger');
const requestPromise = require('request-promise');
const config = require('config');
const ctRegisterMicroservice = require('ct-register-microservice-node');
const CheckData = require('./check-csv-data');


const asyncForEach = async (array, callback) => {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array)
    }
}
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
  
const hdxConfig = {
hdx: {
    "package": "https://data.humdata.org/api/3/action/package_show?id=:package-id",
    "graph": "https://api.hdx.org/v1/dataset/:package-id/vocabulary/knowledge_graph",
    "dataSourceUrl": "https://data.humdata.org/dataset/:package-id",
    "dataSourceEndpoint": "https://data.humdata.org:resouce-file-path"
}
}

class HDXFullIndexService {

    static async cronUpdate() {
        const timeout = ms => new Promise(res => setTimeout(res, ms))
        try {
            logger.info('Running cron update');
            logger.debug('Obtaining datasets');
            const hdxPackageResponse = await requestPromise({
                method: 'GET',
                url: 'https://data.humdata.org/api/action/package_search?&fq=(res_format:JSON%20OR%20res_format:CSV)%20AND%20groups:yem',
                json: true
            });
            let humData = hdxPackageResponse.result;
            let hdxResponse = await ctRegisterMicroservice.requestToMicroservice({
                method: 'GET',
                uri: `/dataset/provider=hdx&page[size]=10000`,
                json: true
            });
            let csvResponse = await ctRegisterMicroservice.requestToMicroservice({
                method: 'GET',
                uri: `/dataset/provider=csv&page[size]=10000`,
                json: true
            });
            
            let tableNames = hdxResponse.data.map(x => { return {tableName: x.attributes.tableName, id: x.id, connectorType: x.attributes.connectorType, name: x.attributes.name}})
            let csvNames = csvResponse.data.map(x => { return {tableName: x.attributes.tableName, id: x.id, connectorType: x.attributes.connectorType, name: x.attributes.name, status: x.attributes.status}})
          
            await asyncForEach(humData.results, async (hdxResult) => {
                await timeout(50)
                await asyncForEach(hdxResult.resources, async (resource) => {
                  await timeout(50)
                  if(resource.mimetype  === 'text/csv' || resource.url.indexOf('csv') > -1){
                    await HDXFullIndexService.addDataset(resource, tableNames, csvNames, hdxResult);  
                  }  
                })
            })        
        } catch (err) {
            logger.error('Error in cron update', err);
            throw err;
        }
    }

    static async addDataset(dataset, tableNamesCodes, csvNames, hdxPackage) { 
        const timeout = ms => new Promise(res => setTimeout(res, ms))
        //iterate through shallow datasets.  if they exist, let's delete and add the full dataset
        if(tableNamesCodes.filter(y => y.name === hdxPackage.name).length > 0) {
          let existingDataset = tableNamesCodes.find(y => y.name === hdxPackage.name);
          try {
            await ctRegisterMicroservice.requestToMicroservice({
                method: 'DELETE',
                uri: `/dataset/${existingDataset.id}`,
                json: true
            });
          } catch (ex) {
            logger.warn(`unable to delete dataset: ${existingDataset.id}`)
            return;
          }
        }
      
        const dataSetName = dataset.name ? dataset.name : dataset.description;
        //iterates through existing full datasets.  If it exists, let's check the data to make sure it was uploaded correctly.  If it's fine, let's skip. if not, delete and re-add
        //if we don't find an existing dataset, lets add it.
        if(csvNames.filter(y => y.name === dataSetName).length > 0) {
          const csv = csvNames.find(y => y.name === dataSetName)  
          if(csv.status === 'failed') {
            //if previously failed, lets delete the dataset and try to re-add
            try {
                await ctRegisterMicroservice.requestToMicroservice({
                    method: 'DELETE',
                    uri: `/dataset/${csv.id}`,
                    json: true
                });    
            } catch (ex) {
                logger.warn(`unable to delete dataset: ${csv.id}`)
                return;    
            }
          }
          else {
            //dataset exists, let's check the data and make sure data is fine 
            let dataIsValid = await CheckData.checkCSVData(csv.id, dataset.url);
            if(dataIsValid) {
              logger.info('Dataset ' + dataset.name + ` with id ${csv.id} already exists, skipping...`);
              return;    
            }
            else {
                logger.warn('Dataset ' + dataset.name + ` with id ${csv.id} exists, but data is corrupted...Deleting and re-adding`);
                try {
                    await ctRegisterMicroservice.requestToMicroservice({
                        method: 'DELETE',
                        uri: `/dataset/${csv.id}`,
                        json: true
                    });    
                } catch (ex) {
                    logger.warn(`unable to delete dataset: ${csv.id}`)
                    return;    
                }
            }
          }
        }
      
        logger.debug('Adding dataset ' + dataset.name)        
        //some descriptions have markdown links, just use the name field
        
        let body = {
            "name": dataSetName,
            "provider": "csv",
            "connectorType": "document",
            "connectorUrl": dataset.url,
            "application":[
              "data4sdgs"
            ],        
            "published": false
        };
        await ctRegisterMicroservice.requestToMicroservice({
            method: 'POST',
            uri: `/dataset`,
            body,
            json: true
        });

        if(!result){
          return;    
        }
        let dataset_id = result.data.id
        let status = 'pending'
      
        while (status == 'pending'){
          //let get_result = await get('v1/dataset/' + dataset_id, api_url, api_token)
          let get_result = await ctRegisterMicroservice.requestToMicroservice({
            method: 'GET',
            uri: `/dataset/${dataset_id}`,
            json: true
          });
          status = get_result.data.attributes.status;
          if (status == 'pending') {
            logger.debug('Sleeping...')
            await timeout(4000)
          }
      
        }
        logger.debug('dataset saved - updating vocab/tags')
        let tags = ['hdx-full-import'];
        const organization = hdxPackage.dataset_source || '';
        await ctRegisterMicroservice.requestToMicroservice({
            method: 'POST',
            uri: `/dataset/${dataset_id}/vocabulary`,
            body: { legacy: { tags: tags } },
            json: true
        });
        //await post({ legacy: { tags: tags } },`v1/dataset/${dataset_id}/vocabulary`, api_url, api_token);
      
      
        const dataSourceUrl = hdxConfig.hdx.dataSourceUrl.replace(':package-id', hdxPackage.name);
        const license = hdxPackage.license_title || hdxPackage.license_id  || '';
        var revisedLicense = ACCEPTED_LICENSE_STRINGS.includes(license.toUpperCase()) ? license : 'Other';
        let metadata = {
          language: 'en',
          name: dataSetName,
          description: dataset.description,
          sourceOrganization: organization,
          dataSourceUrl,
          // dataSourceEndpoint: dataDownloadURL,
          // dataDownloadUrl: dataDownloadURL,
          status: 'published',
          license: revisedLicense,
          userId: result.data.attributes.userId
        };
        if(revisedLicense === 'Other') {
          metadata.info = {
            license: license //TODO: Should this be url? and if so where should it go;
          }
        }
        //logger.debug(metadata)
        await ctRegisterMicroservice.requestToMicroservice({
            method: 'POST',
            uri: `/dataset/${dataset_id}/metadata`,
            body: metadata,
            json: true
        });
        //await post(metadata, `v1/dataset/${result.data.id}/metadata`, api_url, api_token)
      
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
                throw new Error(`Incomplete or invalid data loaded from HDX API`);
            }

            if (hdxPackage.resources.length === 0) {
                throw new Error(`No resource data associated with this HDX package was found`);
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
                throw new Error(`No single JSON or CSV resource found for HDX package`);
            }

            let dataDownloadURL = config.hdx.dataSourceEndpoint.replace(':resouce-file-path', hdxResource.hdx_rel_url);
            //check if hdx_rel_url is actually a relative path.  If it is not, use it as full downloadurl
            if(hdxResource.hdx_rel_url.indexOf('http') === 0){                
                dataDownloadURL = hdxResource.hdx_rel_url; 
            }
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
                        Authorization: `Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjViNzFjZGQzYTYwZTFkNTdmZjVlNzRhYSIsInJvbGUiOiJBRE1JTiIsInByb3ZpZGVyIjoibG9jYWwiLCJlbWFpbCI6ImFwaWhpZ2h3YXlzQGRhdGE0c2Rncy5vcmciLCJleHRyYVVzZXJEYXRhIjp7ImFwcHMiOlsiZGF0YTRzZGdzIl19LCJjcmVhdGVkQXQiOjE1NDA1OTI4NjIyNjYsImlhdCI6MTU0MDU5Mjg2Mn0.S92xKfgEpLihnB-QGEuoSk8Vc2i0An42QEkpmsB1sJQ`,
                        "content-type": "application/json" 
                    },  
                    json: true
                });    
                logger.debug(`setting dataset: ${dataset.id} publish to false ${result}`)
                logger.debug(result)
            }

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

module.exports = HDXFullIndexService;
