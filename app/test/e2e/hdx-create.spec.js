const nock = require('nock');
const chai = require('chai');
const should = chai.should();
const {
    HDX_DATASET_CREATE_REQUEST,
    HDX_API_DATASET_RESPONSE_NO_RESOURCE,
    HDX_API_DATASET_RESOURCE_JSON,
    HDX_API_DATASET_RESOURCE_XLSX,
    HDX_API_DATASET_RESOURCE_CSV
} = require('./test.constants');
const { getTestServer } = require('./test-server');

const requester = getTestServer();

describe('HDX Dataset creation tests', () => {
    before(() => {

        nock.cleanAll();

        // HDX responses for info and metadata on real dataset
        // nock('https://data.humdata.org')
        //     .get(`/api/3/action/package_show?id=${HDX_DATASET_CREATE_REQUEST.connector.tableName}`)
        //     .times(6)
        //     .reply(200, HDX_API_DATASET_RESPONSE_NO_RESOURCE);

        // Metadata update request for real dataset
        // nock(`${process.env.CT_URL}`)
        //     .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, {
        //         dataset: {
        //             status: 1
        //         }
        //     })
        //     .times(2)
        //     .reply(200);
    });

    it('Create dataset for HDX package with no resource should fail', async () => {
        // HDX responses for info on package and resources
        const singleJsonResourceRequest = Object.assign({}, HDX_API_DATASET_RESPONSE_NO_RESOURCE);

        nock('https://data.humdata.org')
            .get(`/api/3/action/package_show?id=${HDX_DATASET_CREATE_REQUEST.connector.tableName}`)
            .once()
            .reply(200, singleJsonResourceRequest);

        nock(`https://api.apihighways.org`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, (request) => {
                const expectedRequestContent = {
                    published: false
                };
    
                request.should.deep.equal(expectedRequestContent);
                return true;
            })
            .once()
            .reply(200);
    

        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, (request) => {
                const expectedRequestContent = {
                    dataset: {
                        status: 2,
                        errorMessage: `Error - Error obtaining metadata: Error: No resource data associated with this HDX package was found: ${HDX_DATASET_CREATE_REQUEST.connector.tableName}`
                    }
                };

                request.should.deep.equal(expectedRequestContent);
                return true;
            })
            .once()
            .reply(200);

            
        const response = await requester
            .post(`/api/v1/hdx/rest-datasets/hdx`)
            .send(HDX_DATASET_CREATE_REQUEST);

        response.status.should.equal(200);
    });

    it('Create dataset for HDX package with a XLSX resource should fail', async () => {
        // HDX responses for info on package and resources
        const singleJsonResourceRequest = Object.assign({}, HDX_API_DATASET_RESPONSE_NO_RESOURCE);
        singleJsonResourceRequest.result.resources = [HDX_API_DATASET_RESOURCE_XLSX];

        nock('https://data.humdata.org')
            .get(`/api/3/action/package_show?id=${HDX_DATASET_CREATE_REQUEST.connector.tableName}`)
            .once()
            .reply(200, singleJsonResourceRequest);

        nock(`https://api.apihighways.org`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, (request) => {
                const expectedRequestContent = {
                    published: false                    
                };
    
                request.should.deep.equal(expectedRequestContent);
                return true;
            })
            .once()
            .reply(200);

        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, (request) => {
                const expectedRequestContent = {
                    dataset: {
                        status: 2,
                        errorMessage: `Error - Error obtaining metadata: Error: No single JSON or CSV resource found for HDX package ${HDX_DATASET_CREATE_REQUEST.connector.tableName}`
                    }
                };

                request.should.deep.equal(expectedRequestContent);
                return true;
            })
            .once()
            .reply(200);

        const response = await requester
            .post(`/api/v1/hdx/rest-datasets/hdx`)
            .send(HDX_DATASET_CREATE_REQUEST);

        response.status.should.equal(200);
    });

    it('Create dataset for HDX package with single JSON resource should be successful (happy case)', async () => {
        // HDX responses for info on package and resources
        const singleJsonResourceRequest = Object.assign({}, HDX_API_DATASET_RESPONSE_NO_RESOURCE);
        singleJsonResourceRequest.result.resources = [HDX_API_DATASET_RESOURCE_JSON];

        nock('https://data.humdata.org')
            .get(`/api/3/action/package_show?id=${HDX_DATASET_CREATE_REQUEST.connector.tableName}`)
            .once()
            .reply(200, singleJsonResourceRequest);

        nock(`${process.env.CT_URL}`)
            .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/metadata`, (body) => {
                const expectedRequestBody = {
                    language: 'en',
                    name: 'Fake HDX package title',
                    description: 'Json resource dataset description',
                    sourceOrganization: 'Redhum Ecuador',
                    dataSourceUrl: 'https://data.humdata.org/dataset/fake-hdx-dataset-name',
                    dataSourceEndpoint: 'https://data.humdata.org/dataset/bdc7d663-7332-4d2d-a5ca-6fc52cae882e/resource/75376c95-ac07-4b08-a551-dd16c70c9f98/download/ecuador_admin_3light.json',
                    dataDownloadUrl: 'https://data.humdata.org/dataset/bdc7d663-7332-4d2d-a5ca-6fc52cae882e/resource/75376c95-ac07-4b08-a551-dd16c70c9f98/download/ecuador_admin_3light.json',
                    status: 'published',
                    license: 'Other',
                    userId: '1a10d7c6e0a37126611fd7a7'
                };

                body.should.deep.equal(expectedRequestBody);
                return true;
            })
            .once()
            .reply(200);

        // nock(`${process.env.CT_URL}`)
        //     .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/vocabulary`, (body) => {
        //         const expectedRequestBody = {
        //             knowledge_graph: {
        //                 tags: [
        //                     'daily',
        //                     'vector',
        //                     'near_real_time',
        //                     'geospatial',
        //                     'table',
        //                     'global',
        //                     'forest',
        //                     'fire'
        //                 ]
        //             },
        //             legacy: {
        //                 tags: ['HDX API']
        //             }
        //         };
        //
        //         body.should.deep.equal(expectedRequestBody);
        //         return true;
        //     })
        //     .once()
        //     .reply(200);

        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, {
                dataset: {
                    status: 1
                }
            })
            .once()
            .reply(200);

        nock(`${process.env.CT_URL}`)
            .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/vocabulary`, (body) => {
                const expectedRequestBody = {
                    legacy: {
                        tags: [
                            'HDX API',
                            'Redhum Ecuador',
                            '4w',
                            'earthquake',
                            'ecuador',
                            'humanitarian'
                        ]
                    }
                };

                body.should.deep.equal(expectedRequestBody);
                return true;
            })
            .once()
            .reply(200);

        const response = await requester
            .post(`/api/v1/hdx/rest-datasets/hdx`)
            .send(HDX_DATASET_CREATE_REQUEST);

        response.status.should.equal(200);
    });

    it('Create dataset for HDX package with single CSV and a single JSON resource should be successful and use the JSON data', async () => {
        // HDX responses for info on package and resources
        const singleJsonResourceRequest = Object.assign({}, HDX_API_DATASET_RESPONSE_NO_RESOURCE);
        singleJsonResourceRequest.result.resources = [HDX_API_DATASET_RESOURCE_JSON, HDX_API_DATASET_RESOURCE_CSV];

        nock('https://data.humdata.org')
            .get(`/api/3/action/package_show?id=${HDX_DATASET_CREATE_REQUEST.connector.tableName}`)
            .once()
            .reply(200, singleJsonResourceRequest);

        nock(`${process.env.CT_URL}`)
            .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/metadata`, (body) => {
                const expectedRequestBody = {
                    language: 'en',
                    name: 'Fake HDX package title',
                    description: 'Json resource dataset description',
                    sourceOrganization: 'Redhum Ecuador',
                    dataSourceUrl: 'https://data.humdata.org/dataset/fake-hdx-dataset-name',
                    dataSourceEndpoint: 'https://data.humdata.org/dataset/bdc7d663-7332-4d2d-a5ca-6fc52cae882e/resource/75376c95-ac07-4b08-a551-dd16c70c9f98/download/ecuador_admin_3light.json',
                    dataDownloadUrl: 'https://data.humdata.org/dataset/bdc7d663-7332-4d2d-a5ca-6fc52cae882e/resource/75376c95-ac07-4b08-a551-dd16c70c9f98/download/ecuador_admin_3light.json',
                    status: 'published',
                    license: 'Other',
                    userId: '1a10d7c6e0a37126611fd7a7'
                };

                body.should.deep.equal(expectedRequestBody);
                return true;
            })
            .once()
            .reply(200);

        // nock(`${process.env.CT_URL}`)
        //     .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/vocabulary`, (body) => {
        //         const expectedRequestBody = {
        //             knowledge_graph: {
        //                 tags: [
        //                     'daily',
        //                     'vector',
        //                     'near_real_time',
        //                     'geospatial',
        //                     'table',
        //                     'global',
        //                     'forest',
        //                     'fire'
        //                 ]
        //             },
        //             legacy: {
        //                 tags: ['HDX API']
        //             }
        //         };
        //
        //         body.should.deep.equal(expectedRequestBody);
        //         return true;
        //     })
        //     .once()
        //     .reply(200);

        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, {
                dataset: {
                    status: 1
                }
            })
            .once()
            .reply(200);

        nock(`${process.env.CT_URL}`)
            .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/vocabulary`, (body) => {
                const expectedRequestBody = {
                    legacy: {
                        tags: [
                            'HDX API',
                            'Redhum Ecuador',
                            '4w',
                            'earthquake',
                            'ecuador',
                            'humanitarian'
                        ]
                    }
                };

                body.should.deep.equal(expectedRequestBody);
                return true;
            })
            .once()
            .reply(200);

        const response = await requester
            .post(`/api/v1/hdx/rest-datasets/hdx`)
            .send(HDX_DATASET_CREATE_REQUEST);

        response.status.should.equal(200);
    });

    it('Create dataset for HDX package with single CSV and no JSON resource should be successful and use the CSV data', async () => {
        // HDX responses for info on package and resources
        const singleJsonResourceRequest = Object.assign({}, HDX_API_DATASET_RESPONSE_NO_RESOURCE);
        singleJsonResourceRequest.result.resources = [HDX_API_DATASET_RESOURCE_CSV];

        nock('https://data.humdata.org')
            .get(`/api/3/action/package_show?id=${HDX_DATASET_CREATE_REQUEST.connector.tableName}`)
            .once()
            .reply(200, singleJsonResourceRequest);

        nock(`${process.env.CT_URL}`)
            .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/metadata`, (body) => {
                const expectedRequestBody = {
                    language: 'en',
                    name: 'Fake HDX package title',
                    description: 'Csv resource dataset description',
                    sourceOrganization: 'Redhum Ecuador',
                    dataSourceUrl: 'https://data.humdata.org/dataset/fake-hdx-dataset-name',
                    dataSourceEndpoint: 'https://data.humdata.org/dataset/614a370e-a34b-42a7-81e7-b08e1d70e4e1/resource/00123612-8469-4da3-aeaf-0f42315545b2/download/160516_5w_forhdx.csv',
                    dataDownloadUrl: 'https://data.humdata.org/dataset/614a370e-a34b-42a7-81e7-b08e1d70e4e1/resource/00123612-8469-4da3-aeaf-0f42315545b2/download/160516_5w_forhdx.csv',
                    status: 'published',
                    license: 'Other',
                    userId: '1a10d7c6e0a37126611fd7a7'
                };

                body.should.deep.equal(expectedRequestBody);
                return true;
            })
            .once()
            .reply(200);

        // nock(`${process.env.CT_URL}`)
        //     .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/vocabulary`, (body) => {
        //         const expectedRequestBody = {
        //             knowledge_graph: {
        //                 tags: [
        //                     'daily',
        //                     'vector',
        //                     'near_real_time',
        //                     'geospatial',
        //                     'table',
        //                     'global',
        //                     'forest',
        //                     'fire'
        //                 ]
        //             },
        //             legacy: {
        //                 tags: ['HDX API']
        //             }
        //         };
        //
        //         body.should.deep.equal(expectedRequestBody);
        //         return true;
        //     })
        //     .once()
        //     .reply(200);

        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, {
                dataset: {
                    status: 1
                }
            })
            .once()
            .reply(200);

        nock(`${process.env.CT_URL}`)
            .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/vocabulary`, (body) => {
                const expectedRequestBody = {
                    legacy: {
                        tags: [
                            'HDX API',
                            'Redhum Ecuador',
                            '4w',
                            'earthquake',
                            'ecuador',
                            'humanitarian'
                        ]
                    }
                };

                body.should.deep.equal(expectedRequestBody);
                return true;
            })
            .once()
            .reply(200);


        const response = await requester
            .post(`/api/v1/hdx/rest-datasets/hdx`)
            .send(HDX_DATASET_CREATE_REQUEST);

        response.status.should.equal(200);
    });


    it('Create dataset for HDX package with single CSV and multiple JSON resource should be successful and use the CSV data', async () => {
        // HDX responses for info on package and resources
        const singleJsonResourceRequest = Object.assign({}, HDX_API_DATASET_RESPONSE_NO_RESOURCE);
        singleJsonResourceRequest.result.resources = [HDX_API_DATASET_RESOURCE_CSV, HDX_API_DATASET_RESOURCE_JSON, HDX_API_DATASET_RESOURCE_JSON];

        nock('https://data.humdata.org')
            .get(`/api/3/action/package_show?id=${HDX_DATASET_CREATE_REQUEST.connector.tableName}`)
            .once()
            .reply(200, singleJsonResourceRequest);

        nock(`${process.env.CT_URL}`)
            .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/metadata`, (body) => {
                const expectedRequestBody = {
                    language: 'en',
                    name: 'Fake HDX package title',
                    description: 'Csv resource dataset description',
                    sourceOrganization: 'Redhum Ecuador',
                    dataSourceUrl: 'https://data.humdata.org/dataset/fake-hdx-dataset-name',
                    dataSourceEndpoint: 'https://data.humdata.org/dataset/614a370e-a34b-42a7-81e7-b08e1d70e4e1/resource/00123612-8469-4da3-aeaf-0f42315545b2/download/160516_5w_forhdx.csv',
                    dataDownloadUrl: 'https://data.humdata.org/dataset/614a370e-a34b-42a7-81e7-b08e1d70e4e1/resource/00123612-8469-4da3-aeaf-0f42315545b2/download/160516_5w_forhdx.csv',
                    status: 'published',
                    license: 'Other',
                    userId: '1a10d7c6e0a37126611fd7a7'
                };

                body.should.deep.equal(expectedRequestBody);
                return true;
            })
            .once()
            .reply(200);

        // nock(`${process.env.CT_URL}`)
        //     .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/vocabulary`, (body) => {
        //         const expectedRequestBody = {
        //             knowledge_graph: {
        //                 tags: [
        //                     'daily',
        //                     'vector',
        //                     'near_real_time',
        //                     'geospatial',
        //                     'table',
        //                     'global',
        //                     'forest',
        //                     'fire'
        //                 ]
        //             },
        //             legacy: {
        //                 tags: ['HDX API']
        //             }
        //         };
        //
        //         body.should.deep.equal(expectedRequestBody);
        //         return true;
        //     })
        //     .once()
        //     .reply(200);

        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, {
                dataset: {
                    status: 1
                }
            })
            .once()
            .reply(200);

        nock(`${process.env.CT_URL}`)
            .post(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}/vocabulary`, (body) => {
                const expectedRequestBody = {
                    legacy: {
                        tags: [
                            'HDX API',
                            'Redhum Ecuador',
                            '4w',
                            'earthquake',
                            'ecuador',
                            'humanitarian'
                        ]
                    }
                };

                body.should.deep.equal(expectedRequestBody);
                return true;
            })
            .once()
            .reply(200);


        const response = await requester
            .post(`/api/v1/hdx/rest-datasets/hdx`)
            .send(HDX_DATASET_CREATE_REQUEST);

        response.status.should.equal(200);
    });

    it('Create dataset for HDX package with multiple CSV and JSON resources should fail', async () => {
        // HDX responses for info on package and resources
        const singleJsonResourceRequest = Object.assign({}, HDX_API_DATASET_RESPONSE_NO_RESOURCE);
        singleJsonResourceRequest.result.resources = [
            HDX_API_DATASET_RESOURCE_JSON,
            HDX_API_DATASET_RESOURCE_JSON,
            HDX_API_DATASET_RESOURCE_CSV,
            HDX_API_DATASET_RESOURCE_CSV
        ];

        nock('https://data.humdata.org')
            .get(`/api/3/action/package_show?id=${HDX_DATASET_CREATE_REQUEST.connector.tableName}`)
            .once()
            .reply(200, singleJsonResourceRequest);

        nock(`https://api.apihighways.org`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, (request) => {
                const expectedRequestContent = {
                    published: false
                };
    
                request.should.deep.equal(expectedRequestContent);
                return true;
            })
            .once()
            .reply(200);

        nock(`${process.env.CT_URL}`)
            .patch(`/v1/dataset/${HDX_DATASET_CREATE_REQUEST.connector.id}`, (request) => {
                const expectedRequestContent = {
                    dataset: {
                        status: 2,
                        errorMessage: `Error - Error obtaining metadata: Error: No single JSON or CSV resource found for HDX package ${HDX_DATASET_CREATE_REQUEST.connector.tableName}`
                    }
                };

                request.should.deep.equal(expectedRequestContent);
                return true;
            })
            .once()
            .reply(200);

        const response = await requester
            .post(`/api/v1/hdx/rest-datasets/hdx`)
            .send(HDX_DATASET_CREATE_REQUEST);

        response.status.should.equal(200);
    });

    afterEach(() => {
        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }
    });
});
