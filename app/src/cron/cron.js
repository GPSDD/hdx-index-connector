const CronJob = require('cron').CronJob;
const logger = require('logger');
const hdxService = require('services/hdx.service');
const hdxFullService = require('services/hdx-full.service');
const config = require('config');

logger.info('Initializing cron');
//logger.info(config.cron);

new CronJob("0 0 13 12 * *", async () => {
    return await hdxService.cronUpdate();
}, null,
  true, /* Start the job right now */
  'America/New_York' /* Time zone of this job. */
);

new CronJob("0 14 9 13 * *", async () => {
  return await hdxFullService.cronUpdate();
}, null,
true, /* Start the job right now */
'America/New_York' /* Time zone of this job. */
);
