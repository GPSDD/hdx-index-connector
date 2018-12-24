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

const response = {
    "help": "http://data.humdata.org/api/3/action/help_show?name=package_search",
    "success": true,
    "result": {
        "count": 28,
        "sort": "score desc, metadata_modified desc",
        "search_facets": {},
        "facets": {},
        "expanded": {},
        "results": [
            {
                "data_update_frequency": "7",
                "license_title": "cc-by-igo",
                "maintainer": "9957c0e9-cd38-40f1-900b-22c91276154b",
                "relationships_as_object": [],
                "private": false,
                "dataset_date": "01/01/1992-07/15/2019",
                "num_tags": 5,
                "id": "4fdcd4dc-5c2f-43af-a1e4-93c9b6539a27",
                "metadata_created": "2015-04-15T21:12:50.572360",
                "caveats": "",
                "metadata_modified": "2018-12-09T08:23:25.470612",
                "author": "",
                "author_email": "",
                "subnational": "0",
                "state": "active",
                "has_geodata": false,
                "methodology": "Registry",
                "version": null,
                "is_requestdata_type": false,
                "license_id": "cc-by-igo",
                "type": "dataset",
                "resources": [
                    {
                        "cache_last_updated": null,
                        "package_id": "4fdcd4dc-5c2f-43af-a1e4-93c9b6539a27",
                        "dataset_preview_enabled": "False",
                        "datastore_active": true,
                        "id": "12d7c8e3-eff9-4db0-93b7-726825c4fe9a",
                        "size": 102740409,
                        "revision_last_updated": "2018-12-09T08:23:25.468852",
                        "state": "active",
                        "hash": "",
                        "description": "Word Food Programme – Food Prices  Data Source: WFP Vulnerability Analysis and Mapping (VAM).",
                        "format": "CSV",
                        "hdx_rel_url": "/dataset/4fdcd4dc-5c2f-43af-a1e4-93c9b6539a27/resource/12d7c8e3-eff9-4db0-93b7-726825c4fe9a/download/wfpvam_foodprices.csv",
                        "last_modified": "2018-12-09T08:23:25.087042",
                        "url_type": "upload",
                        "originalHash": "1476457411",
                        "mimetype": "text/csv",
                        "cache_url": null,
                        "name": "Yemen Test",
                        "created": "2018-09-27T11:46:58.054729",
                        "url": "https://docs.google.com/spreadsheets/d/1HxOT3bSOVGFcQmLA-3wroVlouS1Vy-1lajgnMrR1muA/gviz/tq?tqx=out:csv&sheet=Sheet1",
                        "mimetype_inner": null,
                        "position": 0,
                        "revision_id": "bb8ef885-a37d-439a-b2fe-cecd8a976d89",
                        "resource_type": "file.upload"
                    }
                ],
                "dataset_preview": "first_resource",
                "num_resources": 1,
                "dataset_source": "World Food Programme - VAM",
                "revision_id": "802d8647-eb7e-4455-96e1-fd376532e1ff",
                "creator_user_id": "154de241-38d6-47d3-a77f-0a9848a61df3",
                "has_quickcharts": false,
                "maintainer_email": "orest3.dubay@gmail.com",
                "relationships_as_subject": [],
                "total_res_downloads": 2384,
                "organization": {
                    "description": "WFP is the world's largest humanitarian agency fighting hunger worldwide, delivering food assistance in emergencies and working with communities to improve nutrition and build resilience. Each year, WFP assists some 80 million people in around 75 countries.",
                    "created": "2014-10-24T15:55:52.696098",
                    "title": "WFP - World Food Programme",
                    "name": "wfp",
                    "is_organization": true,
                    "state": "active",
                    "image_url": "",
                    "revision_id": "befd2a5c-7eff-4897-b459-80b00efbf678",
                    "type": "organization",
                    "id": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                    "approval_status": "approved"
                },
                "name": "wfp-food-prices",
                "isopen": false,
                "url": null,
                "notes": "This dataset contains Global Food Prices data from the World Food Programme covering foods such as maize, rice, beans, fish, and sugar for 76 countries and some 1,500 markets. It is updated weekly but contains to a large extent monthly data. The data goes back as far as 1992 for a few countries, although many countries started reporting from 2003 or thereafter.",
                "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                "batch": "5edf1f01-58b4-4973-bada-e35f6930f047",
                "pageviews_last_14_days": 344,
                "title": "Yemen Test",
                "package_creator": "luiscape"
            }
        ]
    }
}
class HDXFullIndexService {

    static async cronUpdate() {
        const timeout = ms => new Promise(res => setTimeout(res, ms))
        try {
            logger.info('Running cron update');
            logger.debug('Obtaining datasets');
            // const hdxPackageResponse = await requestPromise({
            //     method: 'GET',
            //     url: 'https://data.humdata.org/api/action/package_search?&fq=(res_format:JSON%20OR%20res_format:CSV)%20AND%20groups:yem&start=1&rows=100',
            //     json: true
            // });
            const hdxPackageResponse = response;
            let humData = hdxPackageResponse.result;
            let hdxResponse = await ctRegisterMicroservice.requestToMicroservice({
                method: 'GET',
                uri: `/dataset?provider=hdx&page[size]=10000`,
                json: true
            });
            let csvResponse = await ctRegisterMicroservice.requestToMicroservice({
                method: 'GET',
                uri: `/dataset?provider=csv&includes=metadata&page[size]=10000`,
                json: true
            });
            
            let tableNames = hdxResponse.data.map(x => { return {tableName: x.attributes.tableName, id: x.id, connectorType: x.attributes.connectorType, name: x.attributes.name}})
            let csvNames = csvResponse.data.map(x => { 
                if(x.attributes.metadata && x.attributes.metadata.info && x.attributes.metadata.info.hash)
                    return {tableName: x.attributes.tableName, id: x.id, connectorType: x.attributes.connectorType, name: x.attributes.name, status: x.attributes.status, hash: x.attributes.metadata.info.hash};
                return {tableName: x.attributes.tableName, id: x.id, connectorType: x.attributes.connectorType, name: x.attributes.name, status: x.attributes.status, hash: ''}
            })
            
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
        let hash = '';
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
            try {
                let dataIsValid = await CheckData.checkDataValidity(csv.id, dataset.url, csv);
                if(dataIsValid && typeof dataIsValid === 'object' && dataIsValid.match) {
                  logger.info('Dataset ' + dataset.name + ` with id ${csv.id} already exists, skipping...`);
                  return;    
                }                
                else {
                    if(typeof dataIsValid === 'object' && dataIsValid.hash) {
                        hash = dataIsValid.hash;
                    }
                    logger.warn('Dataset ' + dataset.name + ` with id ${csv.id} exists, but data is corrupted or updated...updating`);
                    await HDXFullIndexService.updateDataset(dataset, csv, hash, hdxPackage);
                    return;
                    // try {
                    //     await ctRegisterMicroservice.requestToMicroservice({
                    //         method: 'DELETE',
                    //         uri: `/dataset/${csv.id}`,
                    //         json: true
                    //     });    
                    // } catch (ex) {
                    //     logger.warn(`unable to delete dataset: ${csv.id}`)
                    //     return;    
                    // }
                }    
            } catch (ex) {
                console.warn("Unable to read csv file");
                return;
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
            "overwrite": true,
            "published": false
        };
        let result = await ctRegisterMicroservice.requestToMicroservice({
            method: 'POST',
            uri: `/dataset`,
            body,
            json: true
        });

        if(!result){
          return;    
        }
        logger.debug('new dataset');
        logger.debug(result);
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
        let tags = ['hdx-full-test'];
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
            license: license, //TODO: Should this be url? and if so where should it go;
            hash: hash
          }
        }
        else if(hash.length > 0) {
            metadata.info = {
                hash: hash
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
    static async updateDataset(dataset, csv, hash, hdxPackage) { 
        const timeout = ms => new Promise(res => setTimeout(res, ms))
     
        const dataSetName = dataset.name ? dataset.name : dataset.description;

        logger.debug('updating metadata')
        const dataSourceUrl = hdxConfig.hdx.dataSourceUrl.replace(':package-id', hdxPackage.name);
        const license = hdxPackage.license_title || hdxPackage.license_id  || '';
        var revisedLicense = ACCEPTED_LICENSE_STRINGS.includes(license.toUpperCase()) ? license : 'Other';
        let metadata = {
          name: dataSetName,
          description: dataset.description,
          dataSourceUrl,
          license: revisedLicense,
        };
        if(revisedLicense === 'Other') {
          metadata.info = {
            license: license, //TODO: Should this be url? and if so where should it go;
            hash: hash
          }
        }
        else if(hash.length > 0) {
            metadata.info = {
                hash: hash
            }                
        }
        await ctRegisterMicroservice.requestToMicroservice({
            method: 'PATCH',
            uri: `/dataset/${csv.id}/metadata`,
            body: metadata,
            json: true
        });
        logger.debug(`dataset ${csv.id} updated`)

        logger.debug('Updating dataset data' + dataset.name)        
        //some descriptions have markdown links, just use the name field
        
        let body = {
            "provider": "csv",
            "url": dataset.url
        };
        logger.debug('dataset id ' + csv.id);
        let result = await ctRegisterMicroservice.requestToMicroservice({
            method: 'POST',
            uri: `/dataset/${csv.id}/data-overwrite`,
            body,
            json: true
        });

        logger.debug('dataset update complete');
    }

}

module.exports = HDXFullIndexService;
