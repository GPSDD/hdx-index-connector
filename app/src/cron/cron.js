const CronJob = require('cron').CronJob;
const logger = require('logger');
const hdxService = require('services/hdx.service');
const config = require('config');

logger.info('Initializing cron');
//logger.info(config.cron);

new CronJob("00 2 19 * * 2", async () => {
    return await hdxService.cronUpdate();
}, null,
  true, /* Start the job right now */
  'America/New_York' /* Time zone of this job. */
);
