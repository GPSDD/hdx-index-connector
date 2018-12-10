const requestPromise = require('request-promise');
const ctRegisterMicroservice = require('ct-register-microservice-node');

class CheckData {
  //checks to make sure data was imported properly.  If it has a doctype...it failed
  static async checkCSVData(datasetId, hdxUrl) {
    let get_result = await ctRegisterMicroservice.requestToMicroservice({
      method: 'GET',
      uri: `/query?sql=select%20*%20from%20${datasetId}&format=csv`,
      json: true
    });

    if (!get_result)
    { //dataset doesn't exist...let's skip
      return false;
    }
      
    if(get_result.indexOf('DOCTYPE_html') > -1) {
      console.log('invalid data')
      return false;
    }

    //compare with hdx data
    let result = await requestPromise({
      method: 'GET',
      url: hdxUrl,
    });    
    let newlineRegex = new RegExp('\\r\\n|\\n','g'); 
    if(result.match(newlineRegex).length === get_result.match(newlineRegex)) {
      console.log('data valid')
      return true;  
    }
    return false;
  }

}

module.exports = CheckData