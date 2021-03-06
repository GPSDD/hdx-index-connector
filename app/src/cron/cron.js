const CronJob = require('cron').CronJob;
const logger = require('logger');
const hdxService = require('services/hdx.service');
const hdxFullService = require('services/hdx-full.service');
const config = require('config');

logger.info('Initializing cron');
logger.info('0 6 11 * * *');

new CronJob("0 0 13 12 * *", async () => {
    return await hdxService.cronUpdate();
}, null,
  true, /* Start the job right now */
  'America/New_York' /* Time zone of this job. */
);

//new CronJob("0 0 3 * * *", async () => {
new CronJob("0 20 11 * * *", async () => {
    return await hdxFullService.cronUpdate();
}, null,
true, /* Start the job right now */
'America/New_York' /* Time zone of this job. */
);
