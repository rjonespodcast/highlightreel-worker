import express from 'express';
import cors from 'cors';
import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import os from 'os';

// Worker version - UPDATE THIS WHEN DEPLOYING TO VERIFY CORRECT VERSION
const WORKER_VERSION = '2.1.1';

// Configuration from environment variables
const PORT = process.env.PORT || 3000;
const LOVABLE_API_URL = process.env.LOVABLE_API_URL;
const WORKER_API_KEY = process.env.WORKER_API_KEY;
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '10000');
const MAX_CONCURRENT_DOWNLOADS = parseInt(process.env.MAX_CONCURRENT_DOWNLOADS || '2');

// Validate required environment variables
if (!LOVABLE_API_URL || !WORKER_API_KEY) {
  console.error('❌ Missing required environment variables: LOVABLE_API_URL, WORKER_API_KEY');
  process.exit(1);
}

// Track active downloads
let activeDownloads = 0;
let isProcessing = false;
let stats = {
  processed: 0,
  failed: 0,
  uploaded: 0,
  lastRun: null,
  startedAt: new Date().toISOString()
};

const app = express();
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    version: WORKER_VERSION,
    uploadMethod: 'base64-json',
    message: 'Railway worker is running',
    activeDownloads,
    isProcessing,
    stats,
    uptime: process.uptime()
  });
});

// Manual trigger endpoint
app.post('/process', async (req, res) => {
  if (isProcessing) {
    return res.json({ message: 'Already processing', activeDownloads });
  }
  
  processJobs();
  res.json({ message: 'Processing started', activeDownloads });
});

// Get pending jobs count
app.get('/pending', async (req, res) => {
  try {
    const jobs = await fetchPendingJobs(100);
    res.json({ pending: jobs.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============ Edge Function API Helpers ============

async function fetchPendingJobs(limit = 10) {
  const response = await fetch(`${LOVABLE_API_URL}/worker-get-jobs?limit=${limit}`, {
    method: 'GET',
    headers: { 
      'x-worker-key': WORKER_API_KEY,
      'Content-Type': 'application/json'
    }
  });
  
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Failed to fetch jobs: ${error}`);
  }
  
  const data = await response.json();
  return data.jobs || [];
}

async function updateJobStatus(jobId, updates) {
  const response = await fetch(`${LOVABLE_API_URL}/worker-update-status`, {
    method: 'POST',
    headers: { 
      'x-worker-key': WORKER_API_KEY,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ jobId, ...updates })
  });
  
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Failed to update job status: ${error}`);
  }
  
  return response.json();
}

async function uploadClipToStorage(filePath, jobId, storagePath) {
  // Read file as base64 instead of using FormData
  const fileBuffer = fs.readFileSync(filePath);
  const base64Data = fileBuffer.toString('base64');
  const fileName = path.basename(filePath);
  const fileSize = fileBuffer.length;
  
  console.log(`📤 Uploading ${fileName} (${(fileSize / 1024 / 1024).toFixed(2)} MB) as base64...`);
  
  const response = await fetch(`${LOVABLE_API_URL}/worker-upload-clip`, {
    method: 'POST',
    headers: { 
      'x-worker-key': WORKER_API_KEY,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      file: base64Data,
      jobId,
      storagePath,
      fileName,
      fileSize
    })
  });
  
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Failed to upload clip: ${error}`);
  }
  
  return response.json();
}

// ============ Download Logic ============

// Format seconds to MM:SS
function formatTime(seconds) {
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return `${mins}:${secs.toString().padStart(2, '0')}`;
}

// Run yt-dlp command
async function runYtdlp(job, outputPath) {
  return new Promise((resolve, reject) => {
    const startSeconds = job.quote_start_seconds - job.buffer_start_seconds;
    const endSeconds = job.quote_end_seconds + job.buffer_end_seconds;
    const startTime = formatTime(startSeconds);
    const endTime = formatTime(endSeconds);
    
    const args = [
      job.video_url,
      '-f', 'bv*[height<=1080]+ba/b[height<=1080]/b',
      '--merge-output-format', 'mp4',
      '--download-sections', `*${startTime}-${endTime}`,
      '-o', outputPath,
      '--no-playlist',
      '--quiet',
      '--progress'
    ];
    
    if (job.accurate_cuts) {
      args.push('--force-keyframes-at-cuts');
    }
    
    console.log(`🎬 Running yt-dlp for: ${job.clip_name}`);
    console.log(`   Video: ${job.video_url}`);
    console.log(`   Time: ${startTime} - ${endTime}`);
    
    const ytdlp = spawn('yt-dlp', args);
    
    let stderr = '';
    
    ytdlp.stderr.on('data', (data) => {
      stderr += data.toString();
    });
    
    ytdlp.stdout.on('data', (data) => {
      console.log(`yt-dlp: ${data}`);
    });
    
    ytdlp.on('close', (code) => {
      if (code === 0) {
        console.log(`✅ Downloaded: ${job.clip_name}`);
        resolve(outputPath);
      } else {
        console.error(`❌ yt-dlp failed (code ${code}): ${stderr}`);
        reject(new Error(`yt-dlp exited with code ${code}: ${stderr}`));
      }
    });
    
    ytdlp.on('error', (error) => {
      console.error(`❌ yt-dlp spawn error:`, error.message);
      reject(error);
    });
  });
}

// Process a single job
async function processJob(job) {
  const tempDir = os.tmpdir();
  const sanitizedName = job.clip_name.replace(/[^a-zA-Z0-9-_ ]/g, '').replace(/\s+/g, '_');
  const outputPath = path.join(tempDir, `${job.id}_${sanitizedName}.mp4`);
  const storagePath = `${job.user_id}/${job.id}/${sanitizedName}.mp4`;
  
  try {
    // Update status to downloading
    await updateJobStatus(job.id, {
      status: 'downloading',
      downloadStartedAt: new Date().toISOString(),
      attempts: job.attempts + 1
    });
    
    console.log(`📥 Starting download for job ${job.id}: ${job.clip_name}`);
    
    // Download the clip
    await runYtdlp(job, outputPath);
    
    // Check if file exists and has content
    if (!fs.existsSync(outputPath)) {
      throw new Error('Downloaded file not found');
    }
    
    const fileStats = fs.statSync(outputPath);
    if (fileStats.size === 0) {
      throw new Error('Downloaded file is empty');
    }
    
    console.log(`📤 Uploading to storage: ${storagePath} (${(fileStats.size / 1024 / 1024).toFixed(2)} MB)`);
    
    // Upload to storage via Edge Function
    const uploadResult = await uploadClipToStorage(outputPath, job.id, storagePath);
    console.log(`✅ Completed: ${job.clip_name}`, uploadResult);
    
    stats.uploaded++;
    
  } catch (error) {
    console.error(`❌ Failed: ${job.clip_name}`, error.message);
    
    // Update job as failed
    await updateJobStatus(job.id, {
      status: 'failed',
      errorMessage: error.message
    });
    
    stats.failed++;
    
  } finally {
    // Cleanup temp file
    if (fs.existsSync(outputPath)) {
      fs.unlinkSync(outputPath);
      console.log(`🧹 Cleaned up temp file: ${outputPath}`);
    }
    activeDownloads--;
  }
}

// Claim and process pending jobs
async function processJobs() {
  if (isProcessing) return;
  isProcessing = true;
  stats.lastRun = new Date().toISOString();
  
  try {
    const availableSlots = MAX_CONCURRENT_DOWNLOADS - activeDownloads;
    
    if (availableSlots <= 0) {
      console.log('⏳ No available slots for new downloads');
      return;
    }
    
    // Fetch pending jobs from Edge Function
    const jobs = await fetchPendingJobs(availableSlots);
    
    if (!jobs || jobs.length === 0) {
      return;
    }
    
    console.log(`\n📋 Found ${jobs.length} pending job(s)`);
    
    // Claim and process jobs
    for (const job of jobs) {
      try {
        // Claim the job
        await updateJobStatus(job.id, {
          status: 'claimed',
          claimedAt: new Date().toISOString()
        });
        
        activeDownloads++;
        stats.processed++;
        
        // Process in background (don't await)
        processJob(job).catch(err => {
          console.error(`Error processing job ${job.id}:`, err);
        });
        
      } catch (claimError) {
        console.log(`⚠️ Could not claim job ${job.id}:`, claimError.message);
      }
    }
    
  } catch (error) {
    console.error('❌ Process error:', error.message);
  } finally {
    isProcessing = false;
  }
}

// Start polling
function startPolling() {
  console.log(`🔄 Polling every ${POLL_INTERVAL_MS / 1000}s for new jobs...`);
  
  setInterval(() => {
    if (activeDownloads < MAX_CONCURRENT_DOWNLOADS) {
      processJobs();
    }
  }, POLL_INTERVAL_MS);
  
  // Initial run
  processJobs();
}

// Start server
app.listen(PORT, () => {
  console.log(`
╔═══════════════════════════════════════════════════════════╗
║           HighlightReel Download Worker v2.1              ║
╠═══════════════════════════════════════════════════════════╣
║  🌐 Server: http://localhost:${PORT}                         ║
║  📊 Health: http://localhost:${PORT}/health                  ║
║  ⚡ Trigger: POST http://localhost:${PORT}/process           ║
╠═══════════════════════════════════════════════════════════╣
║  Config:                                                  ║
║  • Poll Interval: ${POLL_INTERVAL_MS}ms                                ║
║  • Max Concurrent: ${MAX_CONCURRENT_DOWNLOADS}                                     ║
║  • API URL: ${LOVABLE_API_URL ? '✅ Connected' : '❌ Missing'}                             ║
║  • Upload: Base64 JSON (v2.1)                             ║
╚═══════════════════════════════════════════════════════════╝
  `);
  
  startPolling();
});
