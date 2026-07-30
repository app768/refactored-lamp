require('dotenv').config();
const { startWorker } = require('./services/transferWorker');

function requireSetting(name) {
  if (!String(process.env[name] || '').trim()) {
    throw new Error(`Missing environment variable: ${name}`);
  }
}

async function main() {
  [
    'QUEUE_API_URL',
    'QUEUE_API_TOKEN',
    'BOT_TOKEN',
    'E2_ENDPOINT',
    'E2_ACCESS_KEY',
    'E2_SECRET_KEY',
  ].forEach(requireSetting);

  console.log('Using Cloudflare D1 transfer queue');
  await startWorker();
}

main().catch((error) => {
  console.error('Fatal startup error:', error);
  process.exit(1);
});
