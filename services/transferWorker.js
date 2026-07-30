const axios = require('axios');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const execFileAsync = promisify(execFile);

const PART_SIZE = (Number(process.env.PART_SIZE_MB) || 25) * 1024 * 1024;
const QUEUE_SIZE = Number(process.env.QUEUE_SIZE) || 4;
const CONCURRENCY = Number(process.env.CONCURRENCY) || 2;
const PROGRESS_INTERVAL_MS = Number(process.env.PROGRESS_INTERVAL_MS) || 7000;
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS) || 2000;

const s3 = new S3Client({
  endpoint: process.env.E2_ENDPOINT,
  region: process.env.E2_REGION || 'us-east-1',
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.E2_ACCESS_KEY,
    secretAccessKey: process.env.E2_SECRET_KEY,
  },
});

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function formatBytes(bytes) {
  if (!bytes) return '0 MB';
  const mb = bytes / (1024 * 1024);
  if (mb >= 1024) return `${(mb / 1024).toFixed(2)} GB`;
  return `${mb.toFixed(1)} MB`;
}

function escapeHtml(text) {
  return String(text).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function filenameOf(key) {
  return key.split('/').pop();
}

function resolutionLabel(width, height) {
  const longEdge = Math.max(Number(width) || 0, Number(height) || 0);
  const shortEdge = Math.min(Number(width) || 0, Number(height) || 0);
  if (!longEdge || !shortEdge) return '';
  if (longEdge >= 3800) return '2160p';
  if (longEdge >= 2500) return '1440p';
  if (longEdge >= 1900) return '1080p';
  if (longEdge >= 1200) return '720p';
  if (longEdge >= 700) return '480p';
  return `${shortEdge}p`;
}

function urlTypeForBucket(bucket) {
  return {
    temp: 'e2_temp',
    movie: 'e2_movie',
    movies: 'e2_movie',
    serie: 'e2_serie',
    series: 'e2_serie',
  }[String(bucket || '').toLowerCase()] || '';
}

function buildDonePayload(job, resolution = '') {
  return {
    name: resolution || '',
    url: filenameOf(job.key),
    url_type: urlTypeForBucket(job.bucket),
    referrer: null,
    user_agent: null,
    custom_script: null,
    kid: null,
    key: null,
    subtitles: [],
  };
}

async function probeVideoResolution(source, objectKey) {
  if (!/\.(?:3gp|avi|flv|m2ts|m4v|mkv|mov|mp4|mpeg|mpg|mts|ts|webm|wmv)$/i.test(
    filenameOf(objectKey),
  )) {
    return '';
  }

  const args = [
    '-v', 'error',
    '-rw_timeout', '15000000',
  ];
  const headerText = Object.entries(source.headers || {})
    .map(([name, value]) => `${name}: ${value}\r\n`)
    .join('');
  if (headerText) args.push('-headers', headerText);
  args.push(
    '-select_streams', 'v:0',
    '-show_entries', 'stream=width,height',
    '-of', 'json',
    source.url,
  );

  try {
    const { stdout } = await execFileAsync('ffprobe', args, {
      timeout: Number(process.env.FFPROBE_TIMEOUT_MS) || 20000,
      maxBuffer: 1024 * 1024,
    });
    const stream = JSON.parse(stdout)?.streams?.[0];
    return resolutionLabel(stream?.width, stream?.height);
  } catch (error) {
    console.warn(`Video resolution unavailable for ${objectKey}: ${error.message}`);
    return '';
  }
}

function extractGoogleDriveFile(rawUrl) {
  let url;
  try {
    url = new URL(rawUrl);
  } catch {
    return null;
  }
  const host = url.hostname.toLowerCase();
  if (host !== 'drive.google.com' && host !== 'drive.usercontent.google.com') {
    return null;
  }
  const pathMatch = url.pathname.match(/\/file\/d\/([A-Za-z0-9_-]+)/i);
  const fileId = pathMatch?.[1] || url.searchParams.get('id');
  if (!fileId || !/^[A-Za-z0-9_-]+$/.test(fileId)) return null;
  return {
    fileId,
    resourceKey: url.searchParams.get('resourcekey') || '',
  };
}

function decodeHtml(text) {
  return String(text)
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>');
}

function contentDispositionFilename(value = '') {
  const encoded = /filename\*\s*=\s*UTF-8''([^;]+)/i.exec(value);
  if (encoded) {
    try {
      return decodeURIComponent(encoded[1].trim().replace(/^"|"$/g, ''));
    } catch {
      // Fall through to the regular filename form.
    }
  }
  const quoted = /filename\s*=\s*"([^"]+)"/i.exec(value);
  if (quoted) return quoted[1];
  const plain = /filename\s*=\s*([^;]+)/i.exec(value);
  return plain ? plain[1].trim() : '';
}

function safeFilename(value) {
  const cleaned = String(value || '')
    .replace(/[/\\\u0000-\u001f]/g, '_')
    .replace(/^\.+/, '')
    .trim();
  return cleaned.slice(0, 240);
}

function cookiesFromResponse(response) {
  const values = response.headers['set-cookie'] || [];
  return (Array.isArray(values) ? values : [values])
    .map((value) => String(value).split(';')[0])
    .filter(Boolean)
    .join('; ');
}

async function streamToText(stream, limit = 2 * 1024 * 1024) {
  const chunks = [];
  let length = 0;
  for await (const chunk of stream) {
    length += chunk.length;
    if (length > limit) {
      stream.destroy();
      throw new Error('Google Drive confirmation page is unexpectedly large');
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf8');
}

function confirmationUrlFromHtml(html, baseUrl) {
  const form = html.match(/<form\b[^>]*\bid=["']download-form["'][^>]*>/i)
    || html.match(/<form\b[^>]*\baction=["'][^"']*download[^"']*["'][^>]*>/i);
  if (form) {
    const action = /\baction=["']([^"']+)["']/i.exec(form[0]);
    if (action) {
      const url = new URL(decodeHtml(action[1]), baseUrl);
      for (const input of html.matchAll(/<input\b[^>]*>/gi)) {
        const name = /\bname=["']([^"']+)["']/i.exec(input[0]);
        const value = /\bvalue=["']([^"']*)["']/i.exec(input[0]);
        if (name) url.searchParams.set(decodeHtml(name[1]), decodeHtml(value?.[1] || ''));
      }
      return url.toString();
    }
  }

  for (const anchor of html.matchAll(/<a\b[^>]*\bhref=["']([^"']+)["'][^>]*>/gi)) {
    const candidate = new URL(decodeHtml(anchor[1]), baseUrl);
    if (
      candidate.hostname === 'drive.usercontent.google.com'
      || candidate.searchParams.has('confirm')
    ) {
      return candidate.toString();
    }
  }
  return '';
}

async function probeGoogleDriveDownload(url, headers = {}) {
  const response = await axios.get(url, {
    responseType: 'stream',
    timeout: 30000,
    maxRedirects: 5,
    headers: { ...headers, Range: 'bytes=0-0' },
    validateStatus: (status) => status >= 200 && status < 400,
  });
  const finalUrl = response.request?.res?.responseUrl || url;
  const disposition = response.headers['content-disposition'] || '';
  const filename = safeFilename(contentDispositionFilename(disposition));
  const contentType = String(response.headers['content-type'] || '').toLowerCase();
  const cookie = [headers.Cookie, cookiesFromResponse(response)].filter(Boolean).join('; ');

  // Google may attach a filename even when the body is only its virus-scan
  // confirmation page. HTML must never be treated as the requested media file.
  if (contentType.includes('text/html')) {
    const page = await streamToText(response.data);
    return {
      confirmationUrl: confirmationUrlFromHtml(page, finalUrl),
      cookie,
    };
  }

  response.data.destroy();
  const range = /\/(\d+)$/.exec(String(response.headers['content-range'] || ''));
  const totalBytes = Number(range?.[1])
    || (response.status === 200 ? Number(response.headers['content-length']) || 0 : 0);
  return {
    url: finalUrl,
    headers: cookie ? { ...headers, Cookie: cookie } : headers,
    filename,
    totalBytes,
    acceptsRanges: response.status === 206
      || String(response.headers['accept-ranges'] || '').toLowerCase() === 'bytes',
  };
}

async function resolveGoogleDriveSource(drive) {
  const download = new URL('https://drive.usercontent.google.com/download');
  download.searchParams.set('id', drive.fileId);
  download.searchParams.set('export', 'download');
  download.searchParams.set('confirm', 't');
  if (drive.resourceKey) download.searchParams.set('resourcekey', drive.resourceKey);

  let url = download.toString();
  let headers = {};
  for (let attempt = 0; attempt < 3; attempt++) {
    const result = await probeGoogleDriveDownload(url, headers);
    if (result.url) return { ...result, fileId: drive.fileId };
    if (!result.confirmationUrl) break;
    url = result.confirmationUrl;
    headers = result.cookie ? { Cookie: result.cookie } : headers;
  }
  throw new Error(
    'Google Drive file cannot be downloaded. Set General access to "Anyone with the link" and allow downloads.',
  );
}

function progressText(job, bytesTransferred, totalBytes, speedBps) {
  const speed = speedBps ? `${formatBytes(speedBps)}/s` : '...';
  if (!totalBytes) {
    return `⏳ ${job.bucket}/${job.key}\n${formatBytes(bytesTransferred)} (size unknown)\nSpeed: ${speed}`;
  }
  const pct = Math.min(100, Math.floor((bytesTransferred / totalBytes) * 100));
  return `⏳ ${job.bucket}/${job.key}\n${pct}% (${formatBytes(bytesTransferred)} / ${formatBytes(totalBytes)})\nSpeed: ${speed}`;
}

function doneText(job, bytesTransferred, resolution = '') {
  const payload = JSON.stringify(buildDonePayload(job, resolution), null, 2);
  return `✅ ${escapeHtml(job.bucket)}/${escapeHtml(job.key)}\nDone (${formatBytes(bytesTransferred)})\n\n<pre><code class="language-json">${escapeHtml(payload)}</code></pre>`;
}

async function telegramEdit(chatId, messageId, text, options = {}) {
  try {
    await axios.post(
      `https://api.telegram.org/bot${process.env.BOT_TOKEN}/editMessageText`,
      {
        chat_id: chatId,
        message_id: Number(messageId),
        text,
        ...options,
      },
      { timeout: 15000 },
    );
  } catch (error) {
    console.warn(`Telegram edit failed: ${error.response?.data?.description || error.message}`);
  }
}

function queueUrl(pathname) {
  const base = process.env.QUEUE_API_URL.endsWith('/')
    ? process.env.QUEUE_API_URL
    : `${process.env.QUEUE_API_URL}/`;
  return new URL(pathname.replace(/^\//, ''), base).toString();
}

async function queueRequest(pathname, body) {
  const response = await fetch(queueUrl(pathname), {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${process.env.QUEUE_API_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  if (response.status === 204) return null;
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(`Queue API ${response.status}: ${JSON.stringify(data)}`);
  }
  return data;
}

async function claimNextJob() {
  return (await queueRequest('/api/jobs/claim'))?.job || null;
}

function updateProgress(jobId, progress) {
  return queueRequest(`/api/jobs/${jobId}/progress`, progress);
}

function completeJob(jobId, result) {
  return queueRequest(`/api/jobs/${jobId}/complete`, result);
}

function prepareJob(jobId, objectKey) {
  return queueRequest(`/api/jobs/${jobId}/prepare`, { objectKey });
}

function failJob(jobId, error) {
  return queueRequest(`/api/jobs/${jobId}/fail`, { error });
}

async function checkUrlAlive(url) {
  const response = await axios.head(url, { timeout: 15000, maxRedirects: 5 });
  return {
    totalBytes: Number(response.headers['content-length']) || 0,
    acceptsRanges: response.headers['accept-ranges'] === 'bytes',
  };
}

async function resolveSource(url) {
  const drive = extractGoogleDriveFile(url);
  if (drive) return resolveGoogleDriveSource(drive);
  const source = await checkUrlAlive(url);
  return { ...source, url, headers: {}, filename: '', fileId: '' };
}

class RangeUnsupportedError extends Error {}

async function runWithConcurrency(tasks, limit) {
  const results = new Array(tasks.length);
  let next = 0;
  async function worker() {
    while (next < tasks.length) {
      const index = next++;
      results[index] = await tasks[index]();
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, worker));
  return results;
}

async function uploadSingleStream(job, source, progress) {
  const response = await axios.get(source.url, {
    responseType: 'stream',
    timeout: 0,
    maxRedirects: 5,
    headers: source.headers,
  });
  response.data.on('data', (chunk) => {
    progress.bytesTransferred += chunk.length;
  });

  const upload = new Upload({
    client: s3,
    partSize: PART_SIZE,
    queueSize: QUEUE_SIZE,
    params: {
      Bucket: job.bucket,
      Key: job.key,
      Body: response.data,
      ContentType: 'application/octet-stream',
    },
  });
  await upload.done();
}

async function uploadParallelRanges(job, source, progress) {
  const totalBytes = source.totalBytes;
  const numberOfParts = Math.ceil(totalBytes / PART_SIZE);
  const { UploadId } = await s3.send(new CreateMultipartUploadCommand({
    Bucket: job.bucket,
    Key: job.key,
    ContentType: 'application/octet-stream',
  }));

  try {
    const tasks = [];
    for (let index = 0; index < numberOfParts; index++) {
      const start = index * PART_SIZE;
      const end = Math.min(start + PART_SIZE, totalBytes) - 1;
      const partNumber = index + 1;

      tasks.push(async () => {
        const response = await axios.get(source.url, {
          responseType: 'stream',
          timeout: 0,
          maxRedirects: 5,
          headers: { ...source.headers, Range: `bytes=${start}-${end}` },
        });
        if (response.status !== 206) {
          response.data.destroy();
          throw new RangeUnsupportedError(`part ${partNumber} range not honored (status ${response.status})`);
        }

        const chunks = [];
        for await (const chunk of response.data) chunks.push(chunk);
        const buffer = Buffer.concat(chunks);
        const expectedSize = end - start + 1;
        if (buffer.length !== expectedSize) {
          throw new RangeUnsupportedError(
            `part ${partNumber} size mismatch (got ${buffer.length}b, expected ${expectedSize}b)`,
          );
        }
        progress.bytesTransferred += buffer.length;

        const result = await s3.send(new UploadPartCommand({
          Bucket: job.bucket,
          Key: job.key,
          UploadId,
          PartNumber: partNumber,
          Body: buffer,
        }));
        return { ETag: result.ETag, PartNumber: partNumber };
      });
    }

    const parts = await runWithConcurrency(tasks, QUEUE_SIZE);
    parts.sort((left, right) => left.PartNumber - right.PartNumber);
    await s3.send(new CompleteMultipartUploadCommand({
      Bucket: job.bucket,
      Key: job.key,
      UploadId,
      MultipartUpload: { Parts: parts },
    }));
  } catch (error) {
    await s3.send(new AbortMultipartUploadCommand({
      Bucket: job.bucket,
      Key: job.key,
      UploadId,
    })).catch(() => {});
    throw error;
  }
}

async function processJob(job, slotIndex = 0) {
  const progress = {
    bytesTransferred: 0,
    lastBytes: 0,
    lastTick: Date.now(),
  };
  let progressStopped = false;
  let progressTimer;
  const initialDelay = Math.floor((slotIndex * PROGRESS_INTERVAL_MS) / CONCURRENCY);

  try {
    const source = await resolveSource(job.url);
    const { totalBytes, acceptsRanges } = source;
    const placeholder = source.fileId ? `gdrive_${source.fileId}` : '';
    if (source.filename && placeholder && filenameOf(job.key) === placeholder) {
      const segments = job.key.split('/');
      segments[segments.length - 1] = source.filename;
      job.key = segments.join('/');
      await prepareJob(job.id, job.key);
    }
    const resolution = await probeVideoResolution(source, job.key);

    async function progressTick() {
      if (progressStopped) return;
      const now = Date.now();
      const elapsedSeconds = (now - progress.lastTick) / 1000;
      const speedBps = elapsedSeconds > 0
        ? (progress.bytesTransferred - progress.lastBytes) / elapsedSeconds
        : 0;
      progress.lastBytes = progress.bytesTransferred;
      progress.lastTick = now;
      const percent = totalBytes
        ? Math.min(100, Math.floor((progress.bytesTransferred / totalBytes) * 100))
        : 0;

      try {
        await Promise.all([
          updateProgress(job.id, {
            progress: percent,
            bytesTransferred: progress.bytesTransferred,
            totalBytes,
          }),
          telegramEdit(
            job.chatId,
            job.messageId,
            progressText(job, progress.bytesTransferred, totalBytes, speedBps),
          ),
        ]);
      } catch (error) {
        console.warn(`Progress heartbeat failed for job ${job.id}: ${error.message}`);
      } finally {
        if (!progressStopped) progressTimer = setTimeout(progressTick, PROGRESS_INTERVAL_MS);
      }
    }

    progressTimer = setTimeout(progressTick, initialDelay);

    if (acceptsRanges && totalBytes >= PART_SIZE * 2) {
      try {
        await uploadParallelRanges(job, source, progress);
      } catch (error) {
        if (!(error instanceof RangeUnsupportedError)) throw error;
        console.warn(`Range download unreliable for ${job.key}; using single stream: ${error.message}`);
        progress.bytesTransferred = 0;
        progress.lastBytes = 0;
        progress.lastTick = Date.now();
        await uploadSingleStream(job, source, progress);
      }
    } else {
      await uploadSingleStream(job, source, progress);
    }

    progressStopped = true;
    clearTimeout(progressTimer);
    await completeJob(job.id, {
      bytesTransferred: progress.bytesTransferred,
      totalBytes: totalBytes || progress.bytesTransferred,
    });
    await telegramEdit(
      job.chatId,
      job.messageId,
      doneText(job, progress.bytesTransferred, resolution),
      { parse_mode: 'HTML' },
    );
  } catch (error) {
    progressStopped = true;
    clearTimeout(progressTimer);
    const message = error.message || String(error);
    const result = await failJob(job.id, message).catch((queueError) => {
      console.error(`Could not mark job ${job.id} failed:`, queueError);
      return { status: 'failed', attempts: job.attempts };
    });

    if (result.status === 'pending') {
      await telegramEdit(
        job.chatId,
        job.messageId,
        `⚠️ ${job.bucket}/${job.key}\nFailed, retrying (${result.attempts})\n${message}`,
      );
    } else {
      await telegramEdit(
        job.chatId,
        job.messageId,
        `❌ ${job.bucket}/${job.key}\nFailed after ${result.attempts} attempts\n${message}`,
      );
    }
  }
}

async function workerLoop(slotIndex) {
  while (true) {
    try {
      const job = await claimNextJob();
      if (!job) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }
      console.log(`Claimed job ${job.id}: ${job.bucket}/${job.key}`);
      await processJob(job, slotIndex);
    } catch (error) {
      console.error(`Worker slot ${slotIndex} error:`, error);
      await sleep(POLL_INTERVAL_MS);
    }
  }
}

async function startWorker() {
  console.log(
    `D1 queue transfer worker started (concurrency=${CONCURRENCY}, partSize=${PART_SIZE / (1024 * 1024)}MB, queueSize=${QUEUE_SIZE})`,
  );
  await Promise.all(Array.from({ length: CONCURRENCY }, (_, index) => workerLoop(index)));
}

module.exports = {
  startWorker,
  buildDonePayload,
  confirmationUrlFromHtml,
  contentDispositionFilename,
  extractGoogleDriveFile,
  formatBytes,
  probeGoogleDriveDownload,
  progressText,
  resolutionLabel,
  safeFilename,
  urlTypeForBucket,
};
