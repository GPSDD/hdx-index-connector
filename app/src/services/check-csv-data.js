const logger = require('logger');
const requestPromise = require('request-promise');
const ctRegisterMicroservice = require('ct-register-microservice-node');
const download = require('download');
const fs = require('fs');
const md5 = require('md5-file')

class CheckData {
  //checks to make sure data was imported properly.  If it has a doctype...it failed
  static async checkCSVData(datasetId, url, dataset) {
    let get_result;
    try {
      get_result = await ctRegisterMicroservice.requestToMicroservice({
        method: 'GET',
        uri: `/query?sql=select%20*%20from%20${datasetId}&format=csv`,
        json: true
      });  
    } catch (ex) {
      logger.warn('dataset does not exist in csv format')
      return false;
    }

    if (!get_result)
    { //dataset doesn't exist...let's skip
      logger.warn('dataset does not exist in csv format')
      return false;
    }
      
    if(get_result.indexOf('DOCTYPE_html') > -1) {
      logger.warn('invalid data')
      return false;
    }

    //compare with hdx data
    try {
      if(!dataset.hash || dataset.hash === "")
        return false;
      let fileHash = await download(url).then(data => {
        fs.writeFileSync(`/tmp/temp.csv`, data);
        const hash = md5.sync(downloadPath)
        return hash;
      })
      if(fileHash === dataset.hash) {
        return true;
      }
      return {hash: fileHash, match: false};        
    }
    catch (ex) {
      logger.error('unable to read csv data')
      logger.error(ex);
      throw new Error(ex)      
    }
  }

}

module.exports = CheckData