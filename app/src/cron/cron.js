const CronJob = require('cron').CronJob;
const logger = require('logger');
const hdxService = require('services/hdx.service');
const config = require('config');

logger.info('Initializing cron');
//logger.info(config.cron);

new CronJob("0 15 12 19 * *", async () => {
    return await hdxService.cronUpdate();
}, null,
  true, /* Start the job right now */
  'America/New_York' /* Time zone of this job. */
);
