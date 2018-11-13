const nock = require('nock');
const chai = require('chai');
const should = chai.should();
const {
    HDX_FAKE_DATASET_CREATE_REQUEST,
} = require('./test.constants');
const { getTestServer } = require('./test-server');

const requester = getTestServer();

describe('HDX fake dataset creation tests ', () => {
    before(() => {
        nock.cleanAll();
    });

    it('Create a dataset for an dataset that doesn\'t exist should return an error', async () => {
        // HDX responses for info and metadata on fake dataset
        nock('https://data.humdata.org')
            .get(`/api/3/action/package_show?id=${HDX_FAKE_DATASET_CREATE_REQUEST.connector.tableName}`)
            .once()
            .reply(404, {
                help: 'http://data.humdata.org/api/3/action/help_show?name=package_show',
                success: false,
                error: {
                    message: 'Not found',
                    __type: 'Not Found Error'
                }
            });

        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_FAKE_DATASET_CREATE_REQUEST.connector.id}`, (request) => {
                const expectedRequestContent = {
                    name: 'Fake HDX package title',
                    published: false,                    
                };
    
                request.should.deep.equal(expectedRequestContent);
                return true;
            })
            .once()
            .reply(200);
        
        // Metadata update request for fake dataset
        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_FAKE_DATASET_CREATE_REQUEST.connector.id}`, (request) => {
                const expectedRequestContent = {
                    dataset: {
                        status: 2,
                        errorMessage: `Error - Error obtaining metadata: StatusCodeError: 404 - {"help":"http://data.humdata.org/api/3/action/help_show?name=package_show","success":false,"error":{"message":"Not found","__type":"Not Found Error"}}`
                    }
                };

                request.should.deep.equal(expectedRequestContent);
                return true;
            })
            .once()
            .reply(200);

        const response = await requester
            .post(`/api/v1/hdx/rest-datasets/hdx`)
            .send(HDX_FAKE_DATASET_CREATE_REQUEST);
        response.status.should.equal(200);
    });

    after(() => {
        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }
    });
});
