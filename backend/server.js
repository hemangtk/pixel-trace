// backend/server.js
require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch'); // v2 style
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());

const RUNPOD_API = process.env.RUNPOD_API || "https://api.runpod.io/v1/pods";
const RUNPOD_KEY = process.env.RUNPOD_API_KEY;

if (!RUNPOD_KEY) {
  console.warn("WARNING: RUNPOD_API_KEY not set in env");
}

// simple in-memory events store (replace with DB for production)
const events = {}; // eventName -> { status, podId, manifest }

app.post('/admin/start-index', async (req, res) => {
  try {
    const { driveFolderId, eventName, ownerId } = req.body;
    if (!driveFolderId || !eventName) return res.status(400).json({ error: "driveFolderId and eventName required" });

    // Save event as queued
    events[eventName] = { status: 'queued', created_at: Date.now() };

    // call RunPod (example) - adapt per your RunPod account/api
    const payload = {
      image: process.env.IMAGE || "yourdocker/pixeltrace-indexer:latest",
      env_vars: {
        DRIVE_FOLDER_ID: driveFolderId,
        EVENT_NAME: eventName,
        OWNER_ID: ownerId || "public",
        CALLBACK_URL: process.env.CALLBACK_URL || (process.env.PUBLIC_URL + '/runpod-callback')
      },
      resources: {
        gpus: parseInt(process.env.GPUS || "1"),
        cpu: parseInt(process.env.CPU || "4"),
        memory: parseInt(process.env.MEM || "8192")
      }
    };

    const rpResp = await fetch(RUNPOD_API, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${RUNPOD_KEY}`
      },
      body: JSON.stringify(payload)
    });

    const data = await rpResp.json();
    events[eventName].status = 'processing';
    events[eventName].pod = data;
    res.json({ ok: true, runpod: data });
  } catch (err) {
    console.error("start-index error:", err);
    res.status(500).json({ error: err.message });
  }
});

// callback endpoint to receive manifest (container posts)
app.post('/runpod-callback', (req, res) => {
  // optional: validate secret header
  const secret = process.env.WEBHOOK_SECRET;
  if (secret) {
    const hdr = req.get('x-runpod-secret') || '';
    if (hdr !== secret) {
      console.warn("Invalid secret on callback");
      return res.status(401).send("unauthorized");
    }
  }

  const manifest = req.body;
  console.log("Received manifest:", manifest);
  const eventName = manifest.event_id;
  if (eventName && events[eventName]) {
    events[eventName].status = 'ready';
    events[eventName].manifest = manifest;
    events[eventName].updated_at = Date.now();
  } else if (eventName) {
    events[eventName] = { status: 'ready', manifest, updated_at: Date.now() };
  }
  res.json({ ok: true });
});

app.get('/events/:eventName', (req, res) => {
  const e = events[req.params.eventName];
  if (!e) return res.status(404).json({ error: "not found" });
  res.json(e);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Admin backend listening on ${PORT}`));
