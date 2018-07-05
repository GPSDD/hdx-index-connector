const ROLES = {
    USER: {
        id: '1a10d7c6e0a37126611fd7a7',
        role: 'USER',
        provider: 'local',
        email: 'user@control-tower.org',
        extraUserData: {
            apps: [
                'hdx',
                'gfw',
                'gfw-climate',
                'prep',
                'aqueduct',
                'forest-atlas',
                'data4sdgs'
            ]
        }
    },
    MANAGER: {
        id: '1a10d7c6e0a37126611fd7a7',
        role: 'MANAGER',
        provider: 'local',
        email: 'user@control-tower.org',
        extraUserData: {
            apps: [
                'hdx',
                'gfw',
                'gfw-climate',
                'prep',
                'aqueduct',
                'forest-atlas',
                'data4sdgs'
            ]
        }
    },
    ADMIN: {
        id: '1a10d7c6e0a37126611fd7a7',
        role: 'ADMIN',
        provider: 'local',
        email: 'user@control-tower.org',
        extraUserData: {
            apps: [
                'hdx',
                'gfw',
                'gfw-climate',
                'prep',
                'aqueduct',
                'forest-atlas',
                'data4sdgs'
            ]
        }
    }
};

const HDX_DATASET_CREATE_REQUEST = {
    connector: {
        __v: 0,
        name: '141121-sierra-leone-health-facilities',
        slug: '141121-sierra-leone-health-facilities',
        connectorType: 'rest',
        provider: 'hdx',
        userId: '1a10d7c6e0a37126611fd7a7',
        updatedAt: '2018-06-01T11:04:47.231Z',
        createdAt: '2018-06-01T11:04:47.231Z',
        legend: {
            nested: [],
            country: [],
            region: [],
            date: []
        },
        taskId: null,
        'protected': false,
        geoInfo: false,
        env: 'production',
        sandbox: false,
        published: true,
        errorMessage: null,
        verified: false,
        overwrite: false,
        status: 'pending',
        tableName: 'fake-hdx-dataset-name',
        connectorUrl: null,
        attributesPath: null,
        dataPath: null,
        application: 'data4sdgs',
        type: null,
        _id: '47e9a243-9aea-44e5-94fc-44866e83a7b0',
        id: '47e9a243-9aea-44e5-94fc-44866e83a7b0',
        connector_url: null,
        attributes_path: null,
        data_path: null,
        table_name: 'fake-hdx-dataset-name'
    },
    userId: '1a10d7c6e0a37126611fd7a7',
    loggedUser: {
        id: 'microservice'
    }
};

const HDX_FAKE_DATASET_CREATE_REQUEST = {
    connector: {
        __v: 0,
        name: 'Fake dataset',
        slug: 'fake-dataset_17',
        connectorType: 'rest',
        provider: 'hdx',
        userId: '1a10d7c6e0a37126611fd7a7',
        updatedAt: '2018-06-01T11:04:47.231Z',
        createdAt: '2018-06-01T11:04:47.231Z',
        legend: {
            nested: [],
            country: [],
            region: [],
            date: []
        },
        taskId: null,
        'protected': false,
        geoInfo: false,
        env: 'production',
        sandbox: false,
        published: true,
        errorMessage: null,
        verified: false,
        overwrite: false,
        status: 'pending',
        tableName: 'fake-dataset-name',
        connectorUrl: null,
        attributesPath: null,
        dataPath: null,
        application: 'data4sdgs',
        type: null,
        _id: '55866e83-9aea-44e5-94fc-44866e83b052',
        id: '55866e83-9aea-44e5-94fc-44866e83b052',
        connector_url: null,
        attributes_path: null,
        data_path: null,
        table_name: 'fake-dataset-name'
    },
    userId: '1a10d7c6e0a37126611fd7a7',
    loggedUser: {
        id: 'microservice'
    }
};

const HDX_API_DATASET_RESOURCE_CSV = {
    cache_last_updated: null,
    package_id: '614a370e-a34b-42a7-81e7-b08e1d70e4e1',
    datastore_active: true,
    id: '00123612-8469-4da3-aeaf-0f42315545b2',
    size: 33591,
    revision_last_updated: '2016-05-26T20:40:22.877251',
    state: 'active',
    hash: '',
    description: 'Csv resource dataset description',
    format: 'CSV',
    hdx_rel_url: '/dataset/614a370e-a34b-42a7-81e7-b08e1d70e4e1/resource/00123612-8469-4da3-aeaf-0f42315545b2/download/160516_5w_forhdx.csv',
    tracking_summary: {
        total: 0,
        recent: 0
    },
    last_modified: null,
    url_type: 'upload',
    position: 1,
    mimetype: null,
    cache_url: null,
    name: '160516_5W_ForHDX.csv',
    created: '2016-05-26T20:38:39.405373',
    url: 'http://data.humdata.org/dataset/614a370e-a34b-42a7-81e7-b08e1d70e4e1/resource/00123612-8469-4da3-aeaf-0f42315545b2/download/160516_5w_forhdx.csv',
    'tracking_summary[recent]': '0',
    mimetype_inner: null,
    'tracking_summary[total]': '0',
    originalHash: '718794167',
    revision_id: '1b53bab7-e664-42fc-a3da-a3e089916cf9',
    resource_type: 'file.upload'
};

const HDX_API_DATASET_RESOURCE_XLSX = {
    cache_last_updated: null,
    package_id: '614a370e-a34b-42a7-81e7-b08e1d70e4e1',
    datastore_active: false,
    id: '999ddda6-6a24-44a9-9a49-facdd32a215b',
    size: 79612,
    revision_last_updated: '2016-05-26T19:31:15.003265',
    state: 'active',
    hash: '',
    description: '',
    format: 'XLSX',
    hdx_rel_url: '/dataset/614a370e-a34b-42a7-81e7-b08e1d70e4e1/resource/999ddda6-6a24-44a9-9a49-facdd32a215b/download/160516_5w_forhdx.xlsx',
    tracking_summary: {
        total: 13,
        recent: 2
    },
    last_modified: null,
    url_type: 'upload',
    originalHash: '97196323',
    mimetype: null,
    cache_url: null,
    name: '160516_5W_ForHDX.xlsx',
    created: '2016-05-23T03:46:07.987569',
    url: 'http://data.humdata.org/dataset/614a370e-a34b-42a7-81e7-b08e1d70e4e1/resource/999ddda6-6a24-44a9-9a49-facdd32a215b/download/160516_5w_forhdx.xlsx',
    mimetype_inner: null,
    position: 0,
    revision_id: '89b1c0f4-ef91-47fb-aea6-c199eaf56c2c',
    resource_type: 'file.upload'
};

const HDX_API_DATASET_RESOURCE_JSON = {
    cache_last_updated: null,
    package_id: 'bdc7d663-7332-4d2d-a5ca-6fc52cae882e',
    datastore_active: false,
    id: '75376c95-ac07-4b08-a551-dd16c70c9f98',
    size: 1559742,
    revision_last_updated: '2016-09-15T11:58:55.657648',
    state: 'active',
    shape_info: '{"state": "success", "error_class": "None", "layer_fields": [{"field_name": "ogc_fid", "data_type": "integer"}, {"field_name": "wkb_geometry", "data_type": "USER-DEFINED"}, {"field_name": "dpa_parroq", "data_type": "character varying"}, {"field_name": "dpa_despar", "data_type": "character varying"}, {"field_name": "dpa_valor", "data_type": "integer"}, {"field_name": "dpa_anio", "data_type": "character varying"}, {"field_name": "dpa_canton", "data_type": "character varying"}, {"field_name": "dpa_descan", "data_type": "character varying"}, {"field_name": "dpa_provin", "data_type": "character varying"}, {"field_name": "dpa_despro", "data_type": "character varying"}], "layer_id": "pre_75376c95_ac07_4b08_a551_dd16c70c9f98", "message": "Import successful", "error_type": "None", "bounding_box": "BOX(-91.6619174857815 -5.01364369591731,-75.1871465547501 1.45208062417419)"}',
    hash: '',
    description: 'Json resource dataset description',
    format: 'JSON',
    hdx_rel_url: '/dataset/bdc7d663-7332-4d2d-a5ca-6fc52cae882e/resource/75376c95-ac07-4b08-a551-dd16c70c9f98/download/ecuador_admin_3light.json',
    tracking_summary: {
        total: 0,
        recent: 0
    },
    last_modified: null,
    url_type: 'upload',
    originalHash: '1051924393',
    mimetype: null,
    cache_url: null,
    name: 'ecuador_admin_3light.json',
    created: '2016-09-15T11:56:05.751025',
    url: 'http://data.humdata.org/dataset/bdc7d663-7332-4d2d-a5ca-6fc52cae882e/resource/75376c95-ac07-4b08-a551-dd16c70c9f98/download/ecuador_admin_3light.json',
    mimetype_inner: null,
    position: 4,
    revision_id: 'c26d918b-7636-44f6-b3d8-0eee62ed447d',
    resource_type: 'file.upload'
};

const HDX_API_DATASET_RESPONSE_NO_RESOURCE = {
    help: 'http://data.humdata.org/api/3/action/help_show?name=package_show',
    success: true,
    result: {
        data_update_frequency: '14',
        license_title: 'other-pd-nr',
        maintainer: '867574ac-ad54-43c3-9729-a8897b3246e6',
        relationships_as_object: [],
        'private': false,
        dataset_date: '05/16/2016',
        num_tags: 4,
        solr_additions: '{"countries": ["Ecuador"]}',
        id: '614a370e-a34b-42a7-81e7-b08e1d70e4e1',
        metadata_created: '2016-05-23T03:46:03.583691',
        methodology_other: '4W',
        metadata_modified: '2016-05-26T20:40:22.880581',
        author: null,
        author_email: null,
        subnational: '1',
        state: 'active',
        has_geodata: false,
        methodology: 'Other',
        version: null,
        is_requestdata_type: false,
        license_id: 'other-pd-nr',
        num_of_showcases: 0,
        type: 'dataset',
        resources: [],
        dataset_preview: 'first_resource',
        num_resources: 2,
        dataset_source: 'Redhum Ecuador',
        tags: [
            {
                vocabulary_id: null,
                state: 'active',
                display_name: '4w',
                id: 'a0e3a59f-b6b9-443b-9f10-63344c9c48ea',
                name: '4w'
            },
            {
                vocabulary_id: null,
                state: 'active',
                display_name: 'earthquake',
                id: '373e99a6-13eb-4b5a-abb2-4bc0f9f939fa',
                name: 'earthquake'
            },
            {
                vocabulary_id: null,
                state: 'active',
                display_name: 'ecuador',
                id: 'edda79a3-c8fa-48de-9597-1bb82ceacb97',
                name: 'ecuador'
            },
            {
                vocabulary_id: null,
                state: 'active',
                display_name: 'humanitarian',
                id: '95dc7f3e-59fd-4a54-9cee-13a5d23f8b45',
                name: 'humanitarian'
            }
        ],
        revision_id: 'ff382675-d7e2-4018-9083-868f12264d06',
        groups: [
            {
                display_name: 'Ecuador',
                description: '',
                image_display_url: '',
                title: 'Ecuador',
                id: 'ecu',
                name: 'ecu'
            }
        ],
        creator_user_id: '867574ac-ad54-43c3-9729-a8897b3246e6',
        has_quickcharts: false,
        relationships_as_subject: [],
        total_res_downloads: 251,
        organization: {
            description: 'Redhum Ecuador',
            created: '2016-05-23T01:59:37.934127',
            title: 'Redhum Ecuador',
            name: 'redhum-ecuador',
            is_organization: true,
            state: 'active',
            image_url: '',
            revision_id: 'f0a0483f-b6ff-460f-abfc-3177b2d52074',
            type: 'organization',
            id: 'fdaaa2b9-5e6e-4790-ba8f-04cdb27e2bbb',
            approval_status: 'approved'
        },
        name: 'Fake HDX package name',
        isopen: false,
        url: null,
        notes: '4W of the Ecuador Response as of 16th May 2016, compiled by OCHA featuring 4W inputs from:\r\nWASH, Food Security, Education, Protection, CCCM, Shelter, Logistics, Early Recovery  sectors. ',
        owner_org: 'fdaaa2b9-5e6e-4790-ba8f-04cdb27e2bbb',
        has_showcases: false,
        pageviews_last_14_days: 1,
        title: 'Fake HDX package title',
        package_creator: 'emilieanne'
    }
};

const HDX_API_VOCABULARY_RESPONSE = {
    data: [
        {
            id: 'knowledge_graph',
            type: 'vocabulary',
            attributes: {
                tags: [
                    'daily',
                    'vector',
                    'near_real_time',
                    'geospatial',
                    'table',
                    'global',
                    'forest',
                    'fire'
                ],
                name: 'knowledge_graph',
                application: 'hdx'
            }
        }
    ]
};

const HDX_API_METADATA_RESPONSE = {
    data: [
        {
            id: '588217d53d81e10b00e6a59f',
            type: 'metadata',
            attributes: {
                dataset: '03a8da41-d8d0-4095-8fd5-267a25d5fc31',
                application: 'data4sdgs',
                resource: {
                    type: 'hdx',
                    id: '03a8da41-d8d0-4095-8fd5-267a25d5fc31'
                },
                language: 'es',
                name: 'Name for HDX in Spanish',
                description: 'GDP per capita is gross domestic product divided by midyear population. GDP is the sum of gross value added by all resident producers in the economy plus any product taxes and minus any subsidies not included in the value of the products. It is calculated without making deductions for depreciation of fabricated assets or for depletion and degradation of natural resources. Data are in current U.S. dollars.',
                source: 'World Bank national accounts data, and OECD National Accounts data files.',
                license: 'Open',
                info: {
                    dataDownload: 'http://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.CD?downloadformat=csv',
                    organization: 'World Bank Group'
                },
                createdAt: '2017-01-20T13:59:49.788Z',
                updatedAt: '2017-01-20T13:59:49.788Z',
                status: 'published'
            }
        },
        {
            id: '588217d53d81e10b00e6a59f',
            type: 'metadata',
            attributes: {
                dataset: '03a8da41-d8d0-4095-8fd5-267a25d5fc31',
                application: 'aqueduct',
                resource: {
                    type: 'dataset',
                    id: '03a8da41-d8d0-4095-8fd5-267a25d5fc31'
                },
                language: 'es',
                name: 'Name for Aqueduct in Spanish',
                description: 'GDP per capita is gross domestic product divided by midyear population. GDP is the sum of gross value added by all resident producers in the economy plus any product taxes and minus any subsidies not included in the value of the products. It is calculated without making deductions for depreciation of fabricated assets or for depletion and degradation of natural resources. Data are in current U.S. dollars.',
                source: 'World Bank national accounts data, and OECD National Accounts data files.',
                license: 'Open',
                info: {
                    dataDownload: 'http://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.CD?downloadformat=csv',
                    organization: 'World Bank Group'
                },
                createdAt: '2017-01-20T13:59:49.788Z',
                updatedAt: '2017-01-20T13:59:49.788Z',
                status: 'published'
            }
        },
        {
            id: '588217d53d81e10b00e6a59f',
            type: 'metadata',
            attributes: {
                dataset: '03a8da41-d8d0-4095-8fd5-267a25d5fc31',
                application: 'aqueduct',
                resource: {
                    type: 'dataset',
                    id: '03a8da41-d8d0-4095-8fd5-267a25d5fc31'
                },
                language: 'en',
                name: 'Name for Aqueduct in English',
                description: 'GDP per capita is gross domestic product divided by midyear population. GDP is the sum of gross value added by all resident producers in the economy plus any product taxes and minus any subsidies not included in the value of the products. It is calculated without making deductions for depreciation of fabricated assets or for depletion and degradation of natural resources. Data are in current U.S. dollars.',
                source: 'World Bank national accounts data, and OECD National Accounts data files.',
                license: 'Open',
                info: {
                    dataDownload: 'http://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.CD?downloadformat=csv',
                    organization: 'World Bank Group'
                },
                createdAt: '2017-01-20T13:59:49.788Z',
                updatedAt: '2017-01-20T13:59:49.788Z',
                status: 'published'
            }
        },
        {
            id: '588217d53d81e10b00e6a59f',
            type: 'metadata',
            attributes: {
                dataset: '03a8da41-d8d0-4095-8fd5-267a25d5fc31',
                application: 'hdx',
                resource: {
                    type: 'dataset',
                    id: '03a8da41-d8d0-4095-8fd5-267a25d5fc31'
                },
                language: 'en',
                name: 'Name for HDX in English',
                description: 'GDP per capita is gross domestic product divided by midyear population. GDP is the sum of gross value added by all resident producers in the economy plus any product taxes and minus any subsidies not included in the value of the products. It is calculated without making deductions for depreciation of fabricated assets or for depletion and degradation of natural resources. Data are in current U.S. dollars.',
                source: 'World Bank national accounts data, and OECD National Accounts data files.',
                license: 'Open',
                info: {
                    dataDownload: 'http://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.CD?downloadformat=csv',
                    organization: 'World Bank Group'
                },
                createdAt: '2017-01-20T13:59:49.788Z',
                updatedAt: '2017-01-20T13:59:49.788Z',
                status: 'published'
            }
        },
        {
            id: '588217d53d81e10b00e6a59f',
            type: 'metadata',
            attributes: {
                dataset: '03a8da41-d8d0-4095-8fd5-267a25d5fc31',
                application: 'gfw',
                resource: {
                    type: 'dataset',
                    id: '03a8da41-d8d0-4095-8fd5-267a25d5fc31'
                },
                language: 'en',
                name: 'Name for GFW in English',
                description: 'GDP per capita is gross domestic product divided by midyear population. GDP is the sum of gross value added by all resident producers in the economy plus any product taxes and minus any subsidies not included in the value of the products. It is calculated without making deductions for depreciation of fabricated assets or for depletion and degradation of natural resources. Data are in current U.S. dollars.',
                source: 'World Bank national accounts data, and OECD National Accounts data files.',
                license: 'Open',
                info: {
                    dataDownload: 'http://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.CD?downloadformat=csv',
                    organization: 'World Bank Group'
                },
                createdAt: '2017-01-20T13:59:49.788Z',
                updatedAt: '2017-01-20T13:59:49.788Z',
                status: 'published'
            }
        },
        {
            id: '588217d53d81e10b00e6a59f',
            type: 'metadata',
            attributes: {
                dataset: '03a8da41-d8d0-4095-8fd5-267a25d5fc31',
                application: 'prep',
                resource: {
                    type: 'dataset',
                    id: '03a8da41-d8d0-4095-8fd5-267a25d5fc31'
                },
                language: 'en',
                name: 'Name for PREP in English',
                description: 'GDP per capita is gross domestic product divided by midyear population. GDP is the sum of gross value added by all resident producers in the economy plus any product taxes and minus any subsidies not included in the value of the products. It is calculated without making deductions for depreciation of fabricated assets or for depletion and degradation of natural resources. Data are in current U.S. dollars.',
                source: 'World Bank national accounts data, and OECD National Accounts data files.',
                license: 'Open',
                info: {
                    dataDownload: 'http://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.CD?downloadformat=csv',
                    organization: 'World Bank Group'
                },
                createdAt: '2017-01-20T13:59:49.788Z',
                updatedAt: '2017-01-20T13:59:49.788Z',
                status: 'published'
            }
        }
    ]
};

module.exports = {
    ROLES,
    HDX_API_DATASET_RESPONSE_NO_RESOURCE,
    HDX_API_METADATA_RESPONSE,
    HDX_DATASET_CREATE_REQUEST,
    HDX_FAKE_DATASET_CREATE_REQUEST,
    HDX_API_VOCABULARY_RESPONSE,
    HDX_API_DATASET_RESOURCE_CSV,
    HDX_API_DATASET_RESOURCE_JSON,
    HDX_API_DATASET_RESOURCE_XLSX
};
