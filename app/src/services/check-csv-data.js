const logger = require('logger');
const requestPromise = require('request-promise');
const ctRegisterMicroservice = require('ct-register-microservice-node');

class CheckData {
  //checks to make sure data was imported properly.  If it has a doctype...it failed
  static async checkCSVData(datasetId, hdxUrl) {
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
    let result = await requestPromise({
      method: 'GET',
      url: hdxUrl,
    });    
    let newlineRegex = new RegExp('\\r\\n|\\n','g'); 
    logger.debug(`hdx csv rows ${result.match(newlineRegex).length}`)
    logger.debug(`api highways csv rows ${get_result.match(newlineRegex).length}`)
    if(result.match(newlineRegex).length === get_result.match(newlineRegex).length 
      || (result.match(newlineRegex).length) === get_result.match(newlineRegex).length + 1) {
      return true;  
    }
    return false;
  }

}

module.exports = CheckData