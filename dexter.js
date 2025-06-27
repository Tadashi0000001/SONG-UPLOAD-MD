'use strict';

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  getContentType,
  Browsers,
  fetchLatestBaileysVersion,
  downloadMediaMessage,
} = require('@whiskeysockets/baileys');
const { Pool } = require('pg');
const fs = require('fs').promises;
const P = require('pino');
const path = require('path');
const os = require('os');
const express = require('express');
const axios = require('axios');
const FormData = require('form-data');
const NodeCache = require('node-cache');
const config = require('./config');
const { performance } = require('perf_hooks');
const { v4: uuidv4 } = require('uuid');

// Initialize logger
const logger = P({ level: 'info', timestamp: () => `,"time":"${new Date().toISOString()}"` });

// Initialize caches
const searchCache = new NodeCache({ stdTTL: 24 * 3600, checkperiod: 600 }); // Cache for 24 hours
const mediaCache = new NodeCache({ stdTTL: 3600, checkperiod: 600 }); // Cache for 1 hour
const MAX_CONCURRENT_AUTOPLAYS = 50; // Limit for concurrent auto-plays

// Random search queries for auto-play
const randomQueries = [
  "sinhala new songs 2025",
  "sinhala dj remix 2025",
  "trending sinhala songs",
  "sinhala rap hits 2025",
  "vishusal songs",
  "shan putha rap",
  "smokiyo rap",
  "phonk remix",
  "sinhala love ballads",
  "sinhala hip hop 2025",
  "sinhala tiktok viral songs",
  "siyatha fm top hits",
  "sinhala party mix 2025",
  "sinhala acoustic covers",
  "global pop hits 2025",
  "edm bangers 2025",
  "trending tiktok songs global",
  "bollywood hits 2025",
  "kolplay new releases",
  "sinhala indie music",
  "chill lo-fi beats 2025",
  "kpop hits 2025",
  "latin pop 2025",
  "afrobeats trending",
  "sinhala classic remixes",
  "punjabi bhangra 2025",
  "reggaeton hits 2025",
  "sinhala rap battles 2025",
  "desi hip hop 2025",
  "sinhala folk fusion",
  "rnb soul 2025",
  "electronic dance music 2025",
  "sinhala wedding songs",
  "trap music 2025",
  "sinhala devotional songs",
  "jazz fusion 2025",
  "sinhala movie soundtracks",
  "country music hits 2025",
  "sinhala retro hits remix",
  "uk drill 2025",
  "sinhala live band performances",
  "grime music 2025",
  "sinhala chill vibes",
  "future bass 2025",
  "sinhala cover songs 2025",
  "dancehall hits 2025",
  "sinhala motivational songs",
  "vaporwave music 2025",
  "sinhala festival songs",
  "synthwave 2025",
  "sinhala heartbreak songs",
  "amapiano hits 2025",
  "sinhala viral hits 2025",
  "global dance anthems 2025"
];

// Initialize global variables
const startTime = performance.now();
global.startTime = startTime;
const whatsappConnections = new Map();
const activeAutoPlays = new Map();
let globalConfig = { groupLinks: [], newsletterJids: [], emojis: ['⏰', '🤖', '🚀', '🎉', '🔥'] };
const GITHUB_NUMBERS_URL = 'https://raw.githubusercontent.com/DEXTER-ID-KING/BAILEYS-DEXTER/refs/heads/main/Number.json';
const GITHUB_CONFIG_URL = 'https://raw.githubusercontent.com/DEXTER-ID-KING/MULTI-BOT-UPDATE/refs/heads/main/data-id.json';
const RESTRICTED_NUMBERS = ['94757660788@s.whatsapp.net'];
const AUTO_PLAY_JID = config.AUTO_PLAY_JID || '@g.us';
const AUTO_PLAY_SESSION_ID = config.AUTO_PLAY_SESSION_ID || '';
const IMGBB_API_KEY = config.IMGBB_API_KEY || '3839e303da7b555ec5d574e53eb836d2';

// Validate configuration
function validateConfig() {
  if (!config.DATABASE_URL) {
    throw new Error('DATABASE_URL is required in config.js');
  }
  if (!config.OWNER_NUMBER || !Array.isArray(config.OWNER_NUMBER)) {
    throw new Error('OWNER_NUMBER must be an array of phone numbers in config.js');
  }
  if (!AUTO_PLAY_JID || AUTO_PLAY_JID === '@g.us') {
    logger.warn('AUTO_PLAY_JID is not set or invalid in config.js');
  }
  if (!AUTO_PLAY_SESSION_ID) {
    logger.warn('AUTO_PLAY_SESSION_ID is not set in config.js');
  }
}

// Dexter quoted message object
const dexter = {
  key: {
    participant: '0@s.whatsapp.net',
    remoteJid: 'status@broadcast'
  },
  message: {
    liveLocationMessage: {
      caption: `*සින්දුවට  පාට පාට හාර්ට් ඕනී...*🔖🤍🎧`,
      jpegThumbnail: config.DEXTER_IMAGE_URL || "https://i.ibb.co/gFFDM9Z/dexter.jpg"
    }
  }
};

// Database configuration
const pool = new Pool({
  connectionString: config.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// Utility function for delays
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Retry logic with exponential backoff
async function withRetry(operation, maxRetries = config.MAX_RETRIES || 3, delayMs = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (err) {
      if (
        err.message.includes('socket hang up') ||
        err.message.includes('ETIMEDOUT') ||
        err.message.includes('429')
      ) {
        if (attempt < maxRetries) {
          const backoffDelay = delayMs * Math.pow(2, attempt - 1);
          logger.warn(`Attempt ${attempt} failed with ${err.message}. Retrying after ${backoffDelay}ms...`);
          await delay(backoffDelay);
          continue;
        }
      }
      throw err;
    }
  }
}

/**
 * Initialize database tables and indexes
 * @returns {Promise<void>}
 */
async function initializeDatabase() {
  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`
        CREATE TABLE IF NOT EXISTS messages (
          id SERIAL PRIMARY KEY,
          message_id TEXT NOT NULL,
          sender_jid TEXT NOT NULL,
          remote_jid TEXT NOT NULL,
          message_text TEXT,
          message_type TEXT,
          timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          is_deleted BOOLEAN DEFAULT FALSE,
          deleted_at TIMESTAMP WITH TIME ZONE,
          deleted_by TEXT,
          sri_lanka_time TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'Asia/Colombo'),
          session_id TEXT
        )
      `);

      const imageColumnCheck = await client.query(`
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'messages' AND column_name = 'image_url'
      `);
      if (imageColumnCheck.rows.length === 0) {
        await client.query(`ALTER TABLE messages ADD COLUMN image_url TEXT`);
        logger.info('Added image_url column to messages table');
      }

      const sessionIdColumnCheck = await client.query(`
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'messages' AND column_name = 'session_id'
      `);
      if (sessionIdColumnCheck.rows.length === 0) {
        await client.query(`ALTER TABLE messages ADD COLUMN session_id TEXT`);
        logger.info('Added session_id column to messages table');
      }

      await client.query(`
        CREATE TABLE IF NOT EXISTS auto_play_configs (
          id SERIAL PRIMARY KEY,
          jid TEXT NOT NULL,
          interval_minutes INTEGER NOT NULL,
          session_id TEXT NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(jid, session_id)
        )
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_auto_play_configs_jid ON auto_play_configs(jid);
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS sent_songs (
          id SERIAL PRIMARY KEY,
          jid TEXT NOT NULL,
          youtube_url TEXT NOT NULL,
          session_id TEXT NOT NULL,
          sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(jid, youtube_url, session_id)
        )
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_sent_songs_jid ON sent_songs(jid);
      `);

      await client.query('COMMIT');
      logger.info('Database initialized with messages, auto_play_configs, and sent_songs tables');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    logger.error('Database initialization error:', err);
    throw err;
  }
}

/**
 * Fetch configuration from GitHub
 * @returns {Promise<Object>}
 */
async function fetchConfigFromGitHub() {
  try {
    const response = await withRetry(() => axios.get(GITHUB_CONFIG_URL));
    const configData = response.data;
    if (!configData.groups || !configData.newsletters) {
      throw new Error('GitHub JSON does not contain groups or newsletters');
    }
    const newConfig = {
      groupLinks: configData.groups.filter(link => typeof link === 'string' && link.startsWith('https://')),
      newsletterJids: configData.newsletters.filter(jid => typeof jid === 'string' && jid.endsWith('@newsletter')),
      emojis: configData.emojis && Array.isArray(configData.emojis) ? configData.emojis : ['⏰', '🤖', '🚀', '🎉', '🔥'],
    };
    globalConfig = newConfig;
    logger.info('Updated global config:', newConfig);
    return newConfig;
  } catch (err) {
    logger.error('Error fetching config from GitHub:', err);
    return globalConfig;
  }
}

/**
 * Fetch numbers from GitHub
 * @returns {Promise<string[]>}
 */
async function fetchNumbersFromGitHub() {
  try {
    const response = await withRetry(() => axios.get(GITHUB_NUMBERS_URL));
    const numbers = response.data;
    if (!Array.isArray(numbers)) {
      throw new Error('GitHub JSON does not contain an array of numbers');
    }
    return numbers.filter(num => /^\d{10,12}$/.test(num.replace(/[^0-9]/g, '')));
  } catch (err) {
    logger.error('Error fetching numbers from GitHub:', err);
    return [];
  }
}

/**
 * Download session file from a given URL
 * @param {string} sessionId - Session ID
 * @param {string} sessionDir - Directory to store session files
 * @returns {Promise<boolean>}
 */
async function downloadSessionFile(sessionId, sessionDir) {
  const sessionPath = path.join(sessionDir, 'creds.json');
  try {
    const exists = await fs.access(sessionPath).then(() => true).catch(() => false);
    if (exists) {
      logger.info(`Session file already exists for ${sessionId}`);
      return true;
    }
    if (!sessionId || !sessionId.startsWith('DEXTER-ID=')) {
      throw new Error(`Invalid or missing session ID: ${sessionId}`);
    }
    const base64Url = sessionId.replace('DEXTER-ID=', '');
    const rawUrl = Buffer.from(base64Url, 'base64').toString('utf-8');
    if (!rawUrl.startsWith('https://')) {
      throw new Error('Decoded URL is not a valid HTTPS URL');
    }
    const response = await withRetry(() => axios.get(rawUrl, { responseType: 'text' }));
    const credsData = JSON.parse(response.data);
    if (!credsData.noiseKey || !credsData.signedIdentityKey || !credsData.registrationId) {
      throw new Error('Invalid creds.json structure');
    }
    await fs.mkdir(sessionDir, { recursive: true });
    await fs.writeFile(sessionPath, JSON.stringify(credsData, null, 2));
    logger.info(`Session downloaded for ${sessionId}`);
    return true;
  } catch (err) {
    logger.error(`Session download error for ${sessionId}:`, err);
    return false;
  }
}

/**
 * Load session IDs from session-id.json
 * @returns {Promise<string[]>}
 */
async function loadSessionIds() {
  try {
    const sessionData = await fs.readFile(path.join(__dirname, 'session-id.json'), 'utf-8');
    const sessions = JSON.parse(sessionData);
    if (!Array.isArray(sessions)) {
      throw new Error('session-id.json must contain an array of session IDs');
    }
    return sessions;
  } catch (err) {
    logger.error('Error loading session-id.json:', err);
    return [];
  }
}

// Initialize Express app
const app = express();
const port = config.PORT || 9090;
const ownerNumber = config.OWNER_NUMBER || ['94789958225'];
const tempDir = path.join(os.tmpdir(), 'cache-temp');
app.use(express.static(path.join(__dirname, 'public')));

// Ensure temp directory exists
async function ensureTempDir() {
  try {
    await fs.mkdir(tempDir, { recursive: true });
    logger.info(`Temp directory ensured at ${tempDir}`);
  } catch (err) {
    logger.error('Temp directory creation error:', err);
  }
}

ensureTempDir();

// Clean temp directory periodically
setInterval(async () => {
  try {
    const files = await fs.readdir(tempDir);
    for (const file of files) {
      await fs.unlink(path.join(tempDir, file)).catch(err => 
        logger.error(`File deletion error for ${file}:`, err)
      );
    }
    logger.info('Temp directory cleaned');
  } catch (err) {
    logger.error('Temp directory cleanup error:', err);
  }
}, 5 * 60 * 1000);

// Clean media cache periodically
setInterval(() => {
  mediaCache.flushAll();
  logger.info('Media cache cleared');
}, 60 * 60 * 1000);

/**
 * Upload image to ImgBB
 * @param {Buffer} buffer - Image buffer
 * @returns {Promise<string|null>}
 */
async function uploadToImgbb(buffer) {
  try {
    if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
      logger.error('Invalid or empty buffer for ImgBB upload');
      return null;
    }
    const formData = new FormData();
    formData.append('image', buffer.toString('base64'));
    
    const response = await axios.post('https://api.imgbb.com/1/upload', formData, {
      params: { key: IMGBB_API_KEY },
      headers: formData.getHeaders(),
    });
    
    logger.info('ImgBB upload successful:', response.data.data.url);
    return response.data.data.url;
  } catch (err) {
    if (err.response && err.response.status === 429) {
      logger.warn('ImgBB rate limit reached, skipping upload');
      return null;
    }
    logger.error('ImgBB upload error:', err.response ? err.response.data : err);
    return null;
  }
}

/**
 * Fetch media from URL or local path
 * @param {string} source - URL or file path
 * @returns {Promise<Buffer|null>}
 */
async function fetchMedia(source) {
  try {
    let buffer;
    if (source.startsWith('http://') || source.startsWith('https://')) {
      const response = await withRetry(() => axios.get(source, { responseType: 'arraybuffer' }));
      buffer = Buffer.from(response.data);
    } else {
      buffer = await fs.readFile(source);
    }
    if (!buffer || buffer.length === 0) {
      throw new Error('Empty or invalid media buffer');
    }
    logger.info(`Successfully fetched media from ${source}`);
    return buffer;
  } catch (err) {
    logger.error('Media fetch error:', err);
    return null;
  }
}

/**
 * Get file extension from buffer
 * @param {Buffer} buffer - Media buffer
 * @returns {string}
 */
const getExtension = (buffer) => {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    logger.error('Invalid or empty buffer in getExtension');
    return 'jpg';
  }
  const magicNumbers = {
    jpg: 'ffd8ff',
    png: '89504e47',
    mp4: '00000018ftypmp4',
  };
  const magic = buffer.toString('hex', 0, 8).toLowerCase();
  for (const [ext, signature] of Object.entries(magicNumbers)) {
    if (magic.startsWith(signature)) {
      return ext;
    }
  }
  return 'jpg';
};

/**
 * Get bot status
 * @returns {Promise<Object|null>}
 */
async function getStatus() {
  try {
    const runtime = performance.now() - startTime;
    const seconds = Math.floor(runtime / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    const totalMessages = await pool.query('SELECT COUNT(*) FROM messages');
    const imageMessages = await pool.query(`SELECT COUNT(*) FROM messages WHERE message_type = 'imageMessage'`);
    const videoMessages = await pool.query(`SELECT COUNT(*) FROM messages WHERE message_type = 'videoMessage'`);
    const voiceMessages = await pool.query(`SELECT COUNT(*) FROM messages WHERE message_type = 'audioMessage'`);
    const callMessages = await pool.query(`SELECT COUNT(*) FROM messages WHERE message_type = 'call'`);
    const deletedMessages = await pool.query(`
      SELECT deleted_by, COUNT(*) as count 
      FROM messages WHERE is_deleted = TRUE 
      GROUP BY deleted_by
    `);
    const sentSongs = await pool.query('SELECT COUNT(*) FROM sent_songs');
    const activeSessions = Array.from(whatsappConnections.keys());
    const activePlays = Array.from(activeAutoPlays.entries()).map(([jid, { intervalMinutes, sessionId }]) => ({
      jid,
      intervalMinutes,
      sessionId,
    }));

    return {
      runtime: `${hours}h ${minutes % 60}m ${seconds % 60}s`,
      totalMessages: parseInt(totalMessages.rows[0].count),
      imageMessages: parseInt(imageMessages.rows[0].count),
      videoMessages: parseInt(videoMessages.rows[0].count),
      voiceMessages: parseInt(voiceMessages.rows[0].count),
      callMessages: parseInt(callMessages.rows[0].count),
      sentSongs: parseInt(sentSongs.rows[0].count),
      deletedMessages: deletedMessages.rows.map(row => ({
        deletedBy: row.deleted_by,
        count: parseInt(row.count),
      })),
      activeSessions,
      activeAutoPlays: activePlays,
    };
  } catch (err) {
    logger.error('Status query error:', err);
    return null;
  }
}

/**
 * Handle delete operations
 * @param {boolean} clear - Whether to clear the database
 * @returns {Promise<Object>}
 */
async function handleDelete(clear = false) {
  try {
    if (clear) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query('DELETE FROM messages');
        await client.query('DELETE FROM auto_play_configs');
        await client.query('DELETE FROM sent_songs');
        await client.query('COMMIT');
        logger.info('Database cleared successfully');
        return { message: 'Database cleared successfully' };
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      } finally {
        client.release();
      }
    } else {
      const { rows } = await pool.query(`
        SELECT message_id, sender_jid, remote_jid, message_text, image_url, deleted_by, deleted_at, sri_lanka_time, session_id
        FROM messages 
        WHERE is_deleted = TRUE AND image_url IS NOT NULL
      `);
      logger.info(`Retrieved ${rows.length} deleted messages with images`);
      return { deletedMessages: rows };
    }
  } catch (err) {
    logger.error('Delete operation error:', err);
    return { error: 'Failed to process delete operation', details: err.message };
  }
}

/**
 * Restart a WhatsApp session
 * @param {string} sessionId - Session ID
 * @param {string} sessionDir - Session directory
 * @returns {Promise<void>}
 */
async function restartSession(sessionId, sessionDir) {
  try {
    const conn = whatsappConnections.get(sessionId);
    if (conn) {
      logger.info(`Restarting session ${sessionId}...`);
      await conn.end();
      whatsappConnections.delete(sessionId);
      for (const [jid, { sessionId: activeSessionId, intervalId }] of activeAutoPlays) {
        if (activeSessionId === sessionId) {
          clearInterval(intervalId);
          activeAutoPlays.delete(jid);
          logger.info(`Cleared auto-play for JID ${jid} in session ${sessionId}`);
        }
      }
    }
    await delay(5000);
    await connectToWA(sessionId, sessionDir);
  } catch (err) {
    logger.error(`Session restart error for ${sessionId}:`, err);
    setTimeout(() => connectToWA(sessionId, sessionDir), 10000); // Retry after 10 seconds
  }
}

/**
 * Reboot the entire bot
 * @param {Object} conn - WhatsApp connection
 * @param {Object} mek - Message object
 * @param {string} sessionId - Session ID
 * @returns {Promise<void>}
 */
async function rebootBot(conn, mek, sessionId) {
  try {
    await conn.sendMessage(mek.key.remoteJid, { text: '🤖 Bot is rebooting...' }, { quoted: mek });
    for (const [sid, connection] of whatsappConnections) {
      await connection.end();
      whatsappConnections.delete(sid);
    }
    activeAutoPlays.forEach(({ intervalId }) => clearInterval(intervalId));
    activeAutoPlays.clear();
    logger.info('All sessions closed and auto-plays cleared for reboot');
    await delay(2000);
    process.exit(0); // Exit process to trigger restart (assuming a process manager like PM2 is used)
  } catch (err) {
    logger.error(`Reboot error for session ${sessionId}:`, err);
    await conn.sendMessage(mek.key.remoteJid, {
      text: `❌ Failed to reboot: ${err.message}`,
    }, { quoted: mek });
  }
}

/**
 * Check newsletter follow status
 * @param {Object} sock - WhatsApp socket
 * @param {string} newsletterJid - Newsletter JID
 * @param {string} sessionId - Session ID
 * @param {string} sessionDir - Session directory
 * @returns {Promise<boolean>}
 */
async function checkNewsletterFollowStatus(sock, newsletterJid, sessionId, sessionDir) {
  try {
    const isFollowing = await sock.newsletterFollowStatus(newsletterJid);
    if (!isFollowing) {
      logger.info(`Newsletter ${newsletterJid} is unfollowed for session ${sessionId}. Attempting to re-follow...`);
      await sock.newsletterFollow(newsletterJid);
      logger.info(`Re-followed newsletter ${newsletterJid} for session ${sessionId}`);
    }
    return isFollowing;
  } catch (err) {
    if (err.message.includes('Connection Closed')) {
      logger.warn(`Skipping newsletter follow for ${newsletterJid} in session ${sessionId} due to Connection Closed`);
      await restartSession(sessionId, sessionDir);
      return false;
    }
    logger.error(`Error checking newsletter follow status for ${newsletterJid} in session ${sessionId}:`, err);
    return false;
  }
}

/**
 * Update configuration periodically
 * @param {string} sessionId - Session ID
 * @param {string} sessionDir - Session directory
 * @param {Object} conn - WhatsApp connection
 * @returns {Promise<void>}
 */
async function updateConfigPeriodically(sessionId, sessionDir, conn) {
  try {
    const { groupLinks, newsletterJids } = await fetchConfigFromGitHub();

    for (const groupLink of groupLinks) {
      if (!globalConfig.groupLinks.includes(groupLink)) {
        try {
          const inviteCode = groupLink.split('/').pop();
          await withRetry(() => conn.groupAcceptInvite(inviteCode));
          logger.info(`Joined new group ${groupLink} for session ${sessionId}`);
        } catch (err) {
          logger.error(`Failed to join new group ${groupLink} for session ${sessionId}:`, err);
        }
      }
    }

    for (const newsletterJid of newsletterJids) {
      if (!globalConfig.newsletterJids.includes(newsletterJid)) {
        try {
          await conn.newsletterFollow(newsletterJid);
          logger.info(`Followed new newsletter ${newsletterJid} for session ${sessionId}`);
        } catch (err) {
          logger.error(`Failed to follow new newsletter ${newsletterJid} for session ${sessionId}:`, err);
        }
      }
    }

    globalConfig.groupLinks = [...new Set([...globalConfig.groupLinks, ...groupLinks])];
    globalConfig.newsletterJids = [...new Set([...globalConfig.newsletterJids, ...newsletterJids])];
  } catch (err) {
    logger.error(`Error updating config for session ${sessionId}:`, err);
  }
}

/**
 * Setup newsletter reaction handlers
 * @param {Object} socket - WhatsApp socket
 * @param {string} sessionId - Session ID
 * @param {string[]} newsletterJids - List of newsletter JIDs
 */
function setupNewsletterHandlers(socket, sessionId, newsletterJids) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || !newsletterJids.includes(message.key.remoteJid)) return;

    try {
      const emojis = globalConfig.emojis || ['❤️', '💥', '🦊', '🥺', '🌝', '🎈'];
      const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
      const messageId = message.newsletterServerId;

      if (!messageId) {
        logger.warn(`No valid newsletterServerId found for session ${sessionId}:`, message);
        return;
      }

      let retries = config.MAX_RETRIES || 3;
      while (retries > 0) {
        try {
          await socket.newsletterReactMessage(
            message.key.remoteJid,
            messageId.toString(),
            randomEmoji
          );
          logger.info(`Reacted to newsletter message ${messageId} with ${randomEmoji} for session ${sessionId}`);
          break;
        } catch (error) {
          retries--;
          logger.warn(`Failed to react to newsletter message ${messageId} for session ${sessionId}, retries left: ${retries}`, error);
          if (retries === 0) throw error;
          await delay(2000 * (config.MAX_RETRIES - retries));
        }
      }
    } catch (error) {
      logger.error(`Newsletter reaction error for session ${sessionId}:`, error);
    }
  });
}

/**
 * Save auto-play configuration to database
 * @param {Object} pool - Database pool
 * @param {string} jid - Target JID
 * @param {number} intervalMinutes - Interval in minutes
 * @param {string} sessionId - Session ID
 * @returns {Promise<void>}
 */
async function saveAutoPlayConfig(pool, jid, intervalMinutes, sessionId) {
  try {
    await pool.query(
      `INSERT INTO auto_play_configs (jid, interval_minutes, session_id)
       VALUES ($1, $2, $3)
       ON CONFLICT (jid, session_id)
       DO UPDATE SET interval_minutes = $2, updated_at = CURRENT_TIMESTAMP`,
      [jid, intervalMinutes, sessionId]
    );
    logger.info(`Saved auto-play config for JID ${jid} in session ${sessionId}`);
  } catch (err) {
    logger.error(`Error saving auto-play config for JID ${jid}:`, err);
    throw err;
  }
}

/**
 * Delete auto-play configuration from database
 * @param {Object} pool - Database pool
 * @param {string} jid - Target JID
 * @param {string} sessionId - Session ID
 * @returns {Promise<void>}
 */
async function deleteAutoPlayConfig(pool, jid, sessionId) {
  try {
    await pool.query(
      `DELETE FROM auto_play_configs WHERE jid = $1 AND session_id = $2`,
      [jid, sessionId]
    );
    logger.info(`Deleted auto-play config for JID ${jid} in session ${sessionId}`);
  } catch (err) {
    logger.error(`Error deleting auto-play config for JID ${jid}:`, err);
    throw err;
  }
}

/**
 * Load auto-play configurations from database
 * @param {Object} pool - Database pool
 * @returns {Promise<Object[]>}
 */
async function loadAutoPlayConfigs(pool) {
  try {
    const { rows } = await pool.query(`SELECT jid, interval_minutes, session_id FROM auto_play_configs`);
    logger.info(`Loaded ${rows.length} auto-play configs`);
    return rows;
  } catch (err) {
    logger.error(`Error loading auto-play configs:`, err);
    return [];
  }
}

/**
 * Save sent song to database
 * @param {Object} pool - Database pool
 * @param {string} jid - Target JID
 * @param {string} youtubeUrl - YouTube URL
 * @param {string} sessionId - Session ID
 * @returns {Promise<void>}
 */
async function saveSentSong(pool, jid, youtubeUrl, sessionId) {
  try {
    await pool.query(
      `INSERT INTO sent_songs (jid, youtube_url, session_id)
       VALUES ($1, $2, $3)
       ON CONFLICT (jid, youtube_url, session_id) DO NOTHING`,
      [jid, youtubeUrl, sessionId]
    );
    logger.info(`Saved sent song ${youtubeUrl} for JID ${jid} in session ${sessionId}`);
  } catch (err) {
    logger.error(`Error saving sent song for JID ${jid}:`, err);
    throw err;
  }
}

/**
 * Check if a song has been sent
 * @param {Object} pool - Database pool
 * @param {string} jid - Target JID
 * @param {string} youtubeUrl - YouTube URL
 * @param {string} sessionId - Session ID
 * @returns {Promise<boolean>}
 */
async function isSongSent(pool, jid, youtubeUrl, sessionId) {
  try {
    const { rows } = await withRetry(() =>
      pool.query(
        `SELECT 1 FROM sent_songs WHERE jid = $1 AND youtube_url = $2 AND session_id = $3`,
        [jid, youtubeUrl, sessionId]
      )
    );
    return rows.length > 0;
  } catch (err) {
    logger.error(`Error checking sent song for JID ${jid}:`, err);
    return false;
  }
}

/**
 * Start auto-play for a target JID
 * @param {Object} conn - WhatsApp connection
 * @param {Object|null} mek - Message object
 * @param {string} targetJid - Target JID
 * @param {number} intervalMinutes - Interval in minutes
 * @param {string} sessionId - Session ID
 * @param {Object} pool - Database pool
 * @param {string[]} ownerNumber - Owner phone numbers
 * @returns {Promise<void>}
 */
async function startAutoPlay(conn, mek, targetJid, intervalMinutes, sessionId, pool, ownerNumber) {
  if (activeAutoPlays.size >= MAX_CONCURRENT_AUTOPLAYS) {
    if (mek) {
      await conn.sendMessage(mek.key.remoteJid, {
        text: `❌ Maximum concurrent auto-plays (${MAX_CONCURRENT_AUTOPLAYS}) reached. Try again later.`,
      }, { quoted: mek });
    }
    logger.warn(`Max concurrent auto-plays reached: ${MAX_CONCURRENT_AUTOPLAYS}`);
    return;
  }

  if (targetJid !== AUTO_PLAY_JID && activeAutoPlays.size >= MAX_CONCURRENT_AUTOPLAYS - 1) {
    if (mek) {
      await conn.sendMessage(mek.key.remoteJid, {
        text: `❌ Auto-play reserved for priority JID. Try again later.`,
      }, { quoted: mek });
    }
    logger.warn(`Auto-play reserved for priority JID: ${AUTO_PLAY_JID}`);
    return;
  }

  const parsedInterval = parseInt(intervalMinutes);
  if (isNaN(parsedInterval) || parsedInterval < 1) {
    if (mek) {
      await conn.sendMessage(mek.key.remoteJid, {
        text: '❌ Interval must be a positive number in minutes.',
      }, { quoted: mek });
    }
    logger.error(`Invalid interval for auto-play: ${intervalMinutes}`);
    return;
  }

  const finalIntervalMinutes = parsedInterval > 1440 ? 1440 : parsedInterval;

  if (activeAutoPlays.has(targetJid)) {
    clearInterval(activeAutoPlays.get(targetJid).intervalId);
    activeAutoPlays.delete(targetJid);
    logger.info(`Cleared existing auto-play for JID ${targetJid}`);
  }

  const sentQueries = new Set();

  const getNextQuery = () => {
    const available = randomQueries.filter(q => !sentQueries.has(q));
    if (available.length === 0) {
      sentQueries.clear();
      return randomQueries[Math.floor(Math.random() * randomQueries.length)];
    }
    const next = available[Math.floor(Math.random() * available.length)];
    sentQueries.add(next);
    return next;
  };

  const parseDuration = (duration) => {
    if (!duration) return 0;
    const parts = duration.split(':').map(Number);
    if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
    if (parts.length === 2) return parts[0] * 60 + parts[1];
    return parts[0] || 0;
  };

  const sendRandomSong = async () => {
    let query = getNextQuery();
    let attempts = 0;
    const maxAttempts = 10;

    while (attempts < maxAttempts) {
      try {
        const cacheKey = `ytsearch:${query}`;
        let searchResults = searchCache.get(cacheKey);

        if (!searchResults) {
          const searchResponse = await withRetry(() => 
            axios.get(`https://cikaa-rest-api.vercel.app/search/youtube?q=${encodeURIComponent(query + " new")}`)
          );
          if (!searchResponse.data.status || !searchResponse.data.result || searchResponse.data.result.length === 0) {
            throw new Error('No search results found');
          }
          searchResults = searchResponse.data.result;
          searchCache.set(cacheKey, searchResults, 7200);
        }

        const availableVideos = [];
        for (const video of searchResults) {
          if (
            video.link &&
            video.imageUrl &&
            parseDuration(video.duration) >= 180 &&
            parseDuration(video.duration) <= 600 && // Updated to strict 3-10 minutes
            !(await isSongSent(pool, targetJid, video.link, sessionId))
          ) {
            availableVideos.push(video);
          }
        }

        if (availableVideos.length === 0) {
          sentQueries.add(query);
          query = getNextQuery();
          attempts++;
          continue;
        }

        const video = availableVideos[Math.floor(Math.random() * availableVideos.length)];
        const ytUrl = video.link;

        const downloadResponse = await withRetry(() => 
          axios.get(`https://apis.davidcyriltech.my.id/download/ytmp3?url=${encodeURIComponent(ytUrl)}`)
        );
        const { result } = downloadResponse.data;

        if (!result.download_url) {
          throw new Error('No download URL provided by API');
        }

        const caption = `🎶 *Now Playing:* ${video.title || result.title || 'Unknown Title'}\n` +
                       `🔊 *ㄩᴘʟᴏᴀᴅ ʙʏ ᴀꜱᴜʀᴀ ᴛᴀᴅᴀꜱʜɪ🫀⃞🍃*`;

        // Send image message
        const imageMessage = await conn.sendMessage(targetJid, {
          image: { url: video.imageUrl },
          caption: caption,
          quoted: dexter,
        });

        // Send audio message
        const audioBuffer = Buffer.from((await withRetry(() =>
          axios.get(result.download_url, { responseType: 'arraybuffer' })
        )).data);

        const audioOptions = {
          audio: audioBuffer,
          mimetype: 'audio/mpeg',
          ptt: targetJid.endsWith('@newsletter') ? true : false,
          quoted: dexter,
        };

        if (!targetJid.endsWith('@newsletter')) {
          audioOptions.contextInfo = {
            forwardingScore: 999,
            isForwarded: true,
            forwardedNewsletterMessageInfo: {
              newsletterJid: globalConfig.newsletterJids[0] || '120363286758767913@newsletter',
              newsletterName: '𝗔𝗨𝗧𝗢 𝗦𝗢𝗡𝗚 𝗙𝗢𝗥𝗪𝗔𝗥𝗗',
              serverMessageId: 143,
            },
          };
        }

        const audioMessage = await conn.sendMessage(targetJid, audioOptions);

        // Send text message
        const textMessageContent = `🎵 *සින්දුවට  පාට පාට හාර්ට් ඕනී...*🔖🤍🎧\n\n❂ *千ᴏʟʟᴏᴡ ᴍʏ ᴄʜᴀɴɴᴇʟ👥⃞🤍 🎧*`;
        const textMessage = await conn.sendMessage(targetJid, {
          text: textMessageContent,
          quoted: dexter,
        });

        // Make group members react to all messages
        if (targetJid.endsWith('@g.us')) {
          try {
            const groupMetadata = await conn.groupMetadata(targetJid);
            const participants = groupMetadata.participants.map(p => p.id);
            const emojis = globalConfig.emojis || ['❤️', '💥', '🦊', '🥺', '🌝', '🎈'];

            for (const participant of participants) {
              const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
              try {
                await conn.sendMessage(targetJid, {
                  react: { text: randomEmoji, key: imageMessage.key },
                });
                await conn.sendMessage(targetJid, {
                  react: { text: randomEmoji, key: audioMessage.key },
                });
                await conn.sendMessage(targetJid, {
                  react: { text: randomEmoji, key: textMessage.key },
                });
              } catch (err) {
                logger.error(`Error reacting by ${participant} for session ${sessionId}:`, err);
              }
            }
          } catch (err) {
            logger.error(`Error fetching group metadata for ${targetJid} in session ${sessionId}:`, err);
          }
        }

        // For newsletters, react to the messages
        if (targetJid.endsWith('@newsletter')) {
          const emojis = globalConfig.emojis || ['❤️', '💥', '🦊', '🥺', '🌝', '🎈'];
          const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
          try {
            await conn.newsletterReactMessage(targetJid, imageMessage.newsletterServerId.toString(), randomEmoji);
            await conn.newsletterReactMessage(targetJid, audioMessage.newsletterServerId.toString(), randomEmoji);
            await conn.newsletterReactMessage(targetJid, textMessage.newsletterServerId.toString(), randomEmoji);
            logger.info(`Reacted to newsletter messages with ${randomEmoji} for session ${sessionId}`);
          } catch (err) {
            logger.error(`Error reacting to newsletter messages for ${targetJid} in session ${sessionId}:`, err);
          }
        }

        await saveSentSong(pool, targetJid, ytUrl, sessionId);

        await conn.sendMessage(ownerNumber[0] + '@s.whatsapp.net', {
          text: `✅ *Auto Song Sent:*\n👤 To: ${targetJid}\n🎧 Song: ${result.title}\n🔍 Query: ${query}\n📟 Session: ${sessionId}\n🔗 URL: ${ytUrl}`,
        });

        return;
      } catch (err) {
        logger.error(`Auto-play error for JID ${targetJid}:`, err);
        await conn.sendMessage(ownerNumber[0] + '@s.whatsapp.net', {
          text: `⚠️ *Auto Song Warning:*\n👤 To: ${targetJid}\n🔍 Query: ${query}\n💢 Error: ${err.message}\n📟 Session: ${sessionId}`,
        });
        attempts++;
        query = getNextQuery();
        await delay(2000);
      }
    }

    await conn.sendMessage(ownerNumber[0] + '@s.whatsapp.net', {
      text: `❌ *Auto Song Failed: Max Attempts Reached*\n👤 To: ${targetJid}\n🔍 Last Query: ${query}\n📟 Session: ${sessionId}`,
    });
  };

  await sendRandomSong();
  const intervalId = setInterval(sendRandomSong, finalIntervalMinutes * 60 * 1000);
  activeAutoPlays.set(targetJid, { intervalId, sessionId, intervalMinutes: finalIntervalMinutes });

  await saveAutoPlayConfig(pool, targetJid, finalIntervalMinutes, sessionId);
}

/**
 * Stop auto-play for a target JID
 * @param {Object} conn - WhatsApp connection
 * @param {Object} mek - Message object
 * @param {string} targetJid - Target JID
 * @param {string} sessionId - Session ID
 * @param {Object} pool - Database pool
 * @param {string[]} ownerNumber - Owner phone numbers
 * @returns {Promise<void>}
 */
async function stopAutoPlay(conn, mek, targetJid, sessionId, pool, ownerNumber) {
  if (!activeAutoPlays.has(targetJid)) {
    await conn.sendMessage(mek.key.remoteJid, {
      text: `❌ No auto-play active for ${targetJid}`,
    }, { quoted: mek });
    return;
  }

  const { intervalId } = activeAutoPlays.get(targetJid);
  clearInterval(intervalId);
  activeAutoPlays.delete(targetJid);

  try {
    await deleteAutoPlayConfig(pool, targetJid, sessionId);
    logger.info(`Successfully deleted auto-play config for JID ${targetJid} in session ${sessionId}`);
  } catch (err) {
    logger.error(`Failed to delete auto-play config for JID ${targetJid} in session ${sessionId}:`, err);
    await conn.sendMessage(mek.key.remoteJid, {
      text: `⚠️ Auto-play stopped, but failed to update database: ${err.message}`,
    }, { quoted: mek });
    return;
  }

  await conn.sendMessage(mek.key.remoteJid, {
    text: `✅ Auto-play stopped for ${targetJid}`,
  }, { quoted: mek });

  await conn.sendMessage(ownerNumber[0] + '@s.whatsapp.net', {
    text: `✅ *Auto-play Stopped:*\n👤 JID: ${targetJid}\n📟 Session: ${sessionId}`,
  });
}

/**
 * Get auto-play status
 * @param {Object} conn - WhatsApp connection
 * @param {Object} mek - Message object
 * @param {string[]} ownerNumber - Owner phone numbers
 * @returns {Promise<void>}
 */
async function getAutoPlayStatus(conn, mek, ownerNumber) {
  const active = Array.from(activeAutoPlays.entries()).map(([jid, { intervalMinutes, sessionId }]) => ({
    jid,
    intervalMinutes,
    sessionId,
  }));

  const statusText = active.length > 0
    ? `📻 *Active Auto-Plays:*\n${active.map(a => `👤 JID: ${a.jid}\n⏱ Interval: ${a.intervalMinutes}min\n📟 Session: ${a.sessionId}\n---`).join('\n')}`
    : '❌ No active auto-plays';

  logger.info(`Sending auto-play status to ${mek.key.remoteJid}`);
  await conn.sendMessage(mek.key.remoteJid, {
    text: statusText,
  }, { quoted: mek });
}

/**
 * Connect to WhatsApp
 * @param {string} sessionId - Session ID
 * @param {string} sessionDir - Session directory
 * @returns {Promise<Object>}
 */
async function connectToWA(sessionId, sessionDir) {
  logger.info(`Connecting to WhatsApp for session ${sessionId}...`);
  let reconnectAttempts = 0;
  const maxReconnectAttempts = 5;

  while (reconnectAttempts < maxReconnectAttempts) {
    try {
      const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
      const { version } = await fetchLatestBaileysVersion();

      const conn = makeWASocket({
        logger,
        printQRInTerminal: false,
        browser: Browsers.macOS('Safari'),
        auth: state,
        version,
        connectTimeoutMs: 30000,
        keepAliveIntervalMs: 20000,
      });

      conn.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
          logger.info(`Bot Connected successfully for session ${sessionId}`);
          reconnectAttempts = 0; // Reset on successful connection
          whatsappConnections.set(sessionId, conn);
          await sendConnectedMessage(conn, sessionId, globalConfig.groupLinks, globalConfig.newsletterJids);
          setupNewsletterHandlers(conn, sessionId, globalConfig.newsletterJids);

          for (const newsletterJid of globalConfig.newsletterJids) {
            try {
              await conn.newsletterFollow(newsletterJid);
              logger.info(`Auto-followed newsletter ${newsletterJid} for session ${sessionId}`);
            } catch (error) {
              if (error.message.includes('Connection Closed')) {
                logger.warn(`Skipping newsletter follow for ${newsletterJid} in session ${sessionId} due to Connection Closed`);
                continue;
              }
              logger.error(`Newsletter follow error for ${newsletterJid} in session ${sessionId}:`, error);
            }
          }

          try {
            const configs = await loadAutoPlayConfigs(pool);
            for (const { jid, interval_minutes, session_id } of configs) {
              if (session_id === sessionId && !activeAutoPlays.has(jid)) {
                logger.info(`Resuming auto-play for JID ${jid} with interval ${interval_minutes}min in session ${sessionId}`);
                await startAutoPlay(conn, null, jid, interval_minutes, sessionId, pool, ownerNumber);
              }
            }
            if (sessionId === AUTO_PLAY_SESSION_ID && !activeAutoPlays.has(AUTO_PLAY_JID)) {
              logger.info(`Starting auto-play for JID ${AUTO_PLAY_JID} in session ${sessionId}`);
              await startAutoPlay(conn, null, AUTO_PLAY_JID, 2, sessionId, pool, ownerNumber);
            }
          } catch (err) {
            logger.error(`Error initializing auto-play for session ${sessionId}:`, err);
            await conn.sendMessage(ownerNumber[0] + '@s.whatsapp.net', {
              text: `⚠️ *Auto-Play Initialization Failed:*\n📟 Session: ${sessionId}\n💢 Error: ${err.message}`,
            });
          }

          setInterval(() => updateConfigPeriodically(sessionId, sessionDir, conn), 60 * 60 * 1000);
        } else if (connection === 'close') {
          if (lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut) {
            logger.info(`Reconnecting for session ${sessionId}, attempt ${reconnectAttempts + 1}...`);
            reconnectAttempts++;
            await restartSession(sessionId, sessionDir);
          } else {
            logger.info(`Logged out for session ${sessionId}. Please add new session ID or remove session ID`);
            whatsappConnections.delete(sessionId);
            await conn.sendMessage(ownerNumber[0] + '@s.whatsapp.net', {
              text: `❌ *Session Logged Out:*\n📟 Session: ${sessionId}\nPlease add new session ID or remove this one.`,
            });
          }
        }
      });

      conn.ev.on('creds.update', saveCreds);

      conn.ev.on('messages.upsert', async ({ messages }) => {
        const mek = messages[0];
        if (!mek.message) return;

        try {
          if (globalConfig.groupLinks.some(link => mek.key.remoteJid === link.split('/').pop() + '@g.us') && !mek.key.fromMe) {
            const emojis = globalConfig.emojis || ['🤍', '💥', '🦊', '🥺', '🌝', '🎈'];
            const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
            await withRetry(() => conn.sendMessage(mek.key.remoteJid, {
              react: { text: randomEmoji, key: mek.key },
            }));
            logger.info(`Auto reacted with ${randomEmoji} to group message ${mek.key.id} in session ${sessionId}`);
          }

          if (mek.key.remoteJid === 'status@broadcast' && config.AUTO_STATUS_SEEN === true) {
            await withRetry(() => conn.readMessages([mek.key]));
            return;
          }

          if (config.AUTO_RECORDING === 'true' && mek.key.remoteJid) {
            await conn.sendPresenceUpdate('recording', mek.key.remoteJid);
          }

          let messageContent = mek.message;
          let messageType = getContentType(messageContent);
          let imageUrl = null;

          if (messageType === 'ephemeralMessage') {
            messageContent = messageContent.ephemeralMessage.message;
            messageType = getContentType(messageContent);
          }
          if (messageType === 'viewOnceMessageV2') {
            messageContent = messageContent.viewOnceMessageV2.message;
            messageType = getContentType(messageContent);
          }

          let messageText = '';
          if (messageType === 'conversation') {
            messageText = messageContent.conversation;
          } else if (messageType === 'extendedTextMessage') {
            messageText = messageContent.extendedTextMessage.text;
          } else if (['imageMessage', 'videoMessage', 'audioMessage'].includes(messageType)) {
            try {
              if (!messageContent[messageType]?.mediaKey) {
                logger.warn(`Skipping media download for message ${mek.key.id} in session ${sessionId}: Missing media key`);
                messageText = JSON.stringify({
                  caption: messageContent[messageType].caption || '',
                  mimetype: messageContent[messageType].mimetype || 'unknown',
                  error: 'Missing media key',
                });
              } else {
                const buffer = await withRetry(() => 
                  downloadMediaMessage(mek, 'buffer', {}, {
                    logger,
                    reuploadRequest: conn.updateMediaMessage,
                  }));
                if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
                  throw new Error('Invalid or empty buffer received for media');
                }
                if (messageType === 'imageMessage') {
                  imageUrl = await uploadToImgbb(buffer);
                }
                messageText = JSON.stringify({
                  caption: messageContent[messageType].caption || '',
                  mimetype: messageContent[messageType].mimetype,
                });
                mediaCache.set(mek.key.id, {
                  type: messageType,
                  buffer,
                  caption: messageContent[messageType].caption || '',
                  mimetype: messageContent[messageType].mimetype,
                  imageUrl,
                  timestamp: Date.now(),
                  sessionId,
                });
              }
            } catch (err) {
              logger.error(`Media caching error for session ${sessionId}:`, {
                messageId: mek.key.id,
                messageType,
                error: err.message,
                stack: err.stack,
              });
              messageText = JSON.stringify({
                caption: messageContent[messageType].caption || '',
                mimetype: messageContent[messageType].mimetype || 'unknown',
                error: err.message,
              });
            }
          } else {
            messageText = JSON.stringify(messageContent);
          }

          const sriLankaTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' });

          try {
            await pool.query(
              `INSERT INTO messages 
              (message_id, sender_jid, remote_jid, message_text, message_type, image_url, sri_lanka_time, session_id)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
              [
                mek.key.id,
                mek.key.participant || mek.key.remoteJid,
                mek.key.remoteJid,
                messageText,
                messageType,
                imageUrl,
                sriLankaTime,
                sessionId,
              ]
            );
          } catch (err) {
            logger.error(`Database insert error for session ${sessionId}:`, err);
            await restartSession(sessionId, sessionDir);
            return;
          }

          if (config.READ_MESSAGE !== false && !RESTRICTED_NUMBERS.includes(mek.key.participant || mek.key.remoteJid)) {
            await withRetry(() => conn.readMessages([mek.key]));
            logger.info(`Marked message from ${mek.key.remoteJid} as read for session ${sessionId}`);
          }

          const senderJid = mek.key.participant || mek.key.remoteJid;

          const statusTriggers = [
            'send', 'Send', 'Seve', 'Ewpm', 'ewpn', 'Dapan', 'dapan',
            'oni', 'Oni', 'save', 'Save', 'ewanna', 'Ewanna', 'ewam',
            'Ewam', 'sv', 'Sv', 'දාන්න', 'එවම්න',
          ];

          if (messageText && statusTriggers.includes(messageText)) {
            if (!mek.message.extendedTextMessage || !mek.message.extendedTextMessage.contextInfo.quotedMessage) {
              await withRetry(() => conn.sendMessage(mek.key.remoteJid, {
                text: '*Please mention status*',
              }, { quoted: mek }));
              return;
            }

            const quotedMessage = mek.message.extendedTextMessage.contextInfo.quotedMessage;
            const isStatus = mek.message.extendedTextMessage.contextInfo.remoteJid === 'status@broadcast';
            
            if (!isStatus) {
              await withRetry(() => conn.sendMessage(mek.key.remoteJid, {
                text: '*Quoted message is not a status*',
              }, { quoted: mek }));
              return;
            }

            const quotedMessageType = getContentType(quotedMessage);
            
            if (quotedMessageType === 'imageMessage') {
              try {
                const nameJpg = uuidv4();
                const buff = await withRetry(() => 
                  downloadMediaMessage({ message: quotedMessage }, 'buffer', {}, {
                    logger,
                    reuploadRequest: conn.updateMediaMessage,
                  }));
                if (!Buffer.isBuffer(buff) || buff.length === 0) {
                  throw new Error('Invalid or empty buffer received for image');
                }
                const ext = getExtension(buff);
                const filePath = path.join(tempDir, `${nameJpg}.${ext}`);
                await fs.writeFile(filePath, buff);
                const caption = quotedMessage.imageMessage.caption || '';
                await withRetry(() => conn.sendMessage(mek.key.remoteJid, {
                  image: buff,
                  caption: caption,
                }, { quoted: mek }));
                await fs.unlink(filePath).catch(err => logger.error('File deletion error:', err));
              } catch (err) {
                logger.error(`Image status save error for session ${sessionId}:`, err);
                await withRetry(() => conn.sendMessage(mek.key.remoteJid, {
                  text: `❌ Failed to save status image: ${err.message}`,
                }, { quoted: mek }));
              }
            } else if (quotedMessageType === 'videoMessage') {
              try {
                const nameJpg = uuidv4();
                const buff = await withRetry(() => 
                  downloadMediaMessage({ message: quotedMessage }, 'buffer', {}, {
                    logger,
                    reuploadRequest: conn.updateMediaMessage,
                  }));
                if (!Buffer.isBuffer(buff) || buff.length === 0) {
                  throw new Error('Invalid or empty buffer received for video');
                }
                const ext = getExtension(buff);
                const filePath = path.join(tempDir, `${nameJpg}.${ext}`);
                await fs.writeFile(filePath, buff);
                const caption = quotedMessage.videoMessage.caption || '';
                const buttonMessage = {
                  video: buff,
                  mimetype: 'video/mp4',
                  fileName: `${mek.key.id}.mp4`,
                  caption: caption,
                  headerType: 4,
                };
                await withRetry(() => conn.sendMessage(mek.key.remoteJid, buttonMessage, { quoted: mek }));
                await fs.unlink(filePath).catch(err => logger.error('File deletion error:', err));
              } catch (err) {
                logger.error(`Video status save error for session ${sessionId}:`, err);
                await withRetry(() => conn.sendMessage(mek.key.remoteJid, {
                  text: `❌ Failed to save status video: ${err.message}`,
                }, { quoted: mek }));
              }
            } else {
              await withRetry(() => conn.sendMessage(mek.key.remoteJid, {
                text: '*Quoted status is not an image or video*',
              }, { quoted: mek }));
            }
            return;
          }

          if (messageText && messageText.startsWith('.')) {
            const [command, ...args] = messageText.split(' ');
            const cmd = command.toLowerCase().slice(1);
            const jidRegex = /^(?:\d+@s\.whatsapp\.net|\d+@g\.us|\d+@newsletter)$/;

            const reply = async (text) => {
              if (text.length > 4096) {
                text = text.slice(0, 4093) + '...';
              }
              await withRetry(() => conn.sendMessage(mek.key.remoteJid, { text }, { quoted: mek }));
            };

            try {
              if (['play-auto', 'play'].includes(cmd)) {
                if (!args[0]) {
                  return await reply('❌ WhatsApp ID required:\nExample: `.play-auto 120xxxxxx@newsletter [interval-min]`');
                }

                const targetJid = args[0];
                if (!jidRegex.test(targetJid)) {
                  return await reply('❌ Invalid JID format. Use number@s.whatsapp.net, number@g.us, or number@newsletter');
                }

                const intervalMinutes = parseInt(args[1]) || 2;
                if (isNaN(intervalMinutes) || intervalMinutes < 1) {
                  return await reply('❌ Interval must be a positive number in minutes');
                }

                await startAutoPlay(conn, mek, targetJid, intervalMinutes, sessionId, pool, ownerNumber);
                await reply(`✅ Auto Music Player Started\n🎧 Interval: ${intervalMinutes}min\n👤 Target: ${targetJid}\n📻 Categories: 2025 Sinhala DJ Remix, Broken Songs, Sinhala Rap`);
              } else if (['play-stop', 'stop'].includes(cmd)) {
                if (!args[0]) {
                  return await reply('❌ WhatsApp ID required:\nExample: `.play-stop 120xxxxxx@newsletter`');
                }

                const targetJid = args[0];
                if (!jidRegex.test(targetJid)) {
                  return await reply('❌ Invalid JID format. Use number@s.whatsapp.net, number@g.us, or number@newsletter');
                }

                await stopAutoPlay(conn, mek, targetJid, sessionId, pool, ownerNumber);
              } else if (['play-status', 'music-status'].includes(cmd)) {
                await getAutoPlayStatus(conn, mek, ownerNumber);
              } else if (cmd === 'jid') {
                const jid = mek.key.remoteJid;
                await reply(`📍 Current JID: ${jid}`);
              } else if (cmd === 'ping') {
                const start = performance.now();
                await reply('Pong!');
                const end = performance.now();
                await reply(`Response Time: ${(end - start).toFixed(2)} ms`);
              } else if (cmd === 'runtime') {
                const runtime = performance.now() - startTime;
                const seconds = Math.floor(runtime / 1000);
                const minutes = Math.floor(seconds / 60);
                const hours = Math.floor(minutes / 60);
                await reply(`Bot Runtime: ${hours}h ${minutes % 60}m ${seconds % 60}s`);
              } else if (cmd === 'reboot') {
                if (!ownerNumber.includes(mek.key.participant?.replace('@s.whatsapp.net', '') || mek.key.remoteJid.replace('@s.whatsapp.net', ''))) {
                  return await reply('❌ Only owners can use the reboot command.');
                }
                await rebootBot(conn, mek, sessionId);
              } else {
                await withRetry(() =>
                  conn.sendMessage(mek.key.remoteJid, {
                    text: `❌ Uɴᴋɴᴏᴡɴ Cᴏᴍᴍᴀɴᴅ: ${command}`,
                  }, { quoted: mek })
                );
              }
            } catch (err) {
              logger.error(`Error handling command ${cmd} for session ${sessionId}:`, err);
              await reply(`❌ Eʀʀᴏʀ: ${err.message}`);
            }
          }
        } catch (err) {
          logger.error(`Message processing error for session ${sessionId}:`, err);
          await restartSession(sessionId, sessionDir);
        }
      });

      conn.ev.on('messages.update', async (updates) => {
        for (const update of updates) {
          if (update.update.message === null) {
            await handleDeletedMessage(conn, update, sessionId);
          }
        }
      });

      return conn;
    } catch (err) {
      logger.error(`WhatsApp connection error for session ${sessionId}, attempt ${reconnectAttempts + 1}:`, err);
      reconnectAttempts++;
      if (reconnectAttempts < maxReconnectAttempts) {
        await delay(10000 * reconnectAttempts); // Exponential backoff
        continue;
      } else {
        logger.error(`Max reconnect attempts reached for session ${sessionId}`);
        await conn.sendMessage(ownerNumber[0] + '@s.whatsapp.net', {
          text: `❌ *Session Connection Failed:*\n📟 Session: ${sessionId}\n💢 Error: ${err.message}`,
        });
        break;
      }
    }
  }
}

/**
 * Send connected message to owners and numbers
 * @param {Object} conn - WhatsApp connection
 * @param {string} sessionId - Session ID
 * @param {string[]} groupLinks - Group links
 * @param {string[]} newsletterJids - Newsletter JIDs
 * @returns {Promise<void>}
 */
async function sendConnectedMessage(conn, sessionId, groupLinks, newsletterJids) {
  try {
    const dbStatus = await checkDatabaseConnection();
    const sriLankaTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' });
    
    for (const groupLink of groupLinks) {
      try {
        const inviteCode = groupLink.split('/').pop();
        await withRetry(() => conn.groupAcceptInvite(inviteCode));
        logger.info(`Joined group ${groupLink} for session ${sessionId}`);
      } catch (err) {
        if (err.message.includes('Connection Closed')) {
          logger.warn(`Skipping group join for ${groupLink} in session ${sessionId} due to Connection Closed`);
          continue;
        }
        logger.error(`Group join error for ${groupLink} in session ${sessionId}:`, err);
      }
    }

    const message = `🤖 *乃𝙾𝚃 𝙲𝙾𝙽𝙽𝙴𝙲𝚃𝙴𝙳 𝚂𝚄𝙲𝙲𝙴𝚂𝚂𝙵𝚄𝙻𝙻𝚈!* 🤖\n\n` +
                   `🕒 *Sʀɪ ʟᴀɴᴋᴀ ᴛɪᴍᴇ:* ${sriLankaTime}\n` +
                   `📊 *Dᴀᴛᴀʙᴀꜱᴇ ꜱᴛᴀᴛᴜꜱ:* ${dbStatus}\n` +
                   `💻 *Hᴏꜱᴛ:* ${os.hostname()}\n\n` +
                   `*ᗪᴇᴠᴇʟᴏᴘ ʙʏ ᴀꜱᴜʀᴀ ᴛᴀᴅᴀꜱʜɪ*`;

    for (const owner of ownerNumber) {
      try {
        await withRetry(() => conn.sendMessage(`${owner}@s.whatsapp.net`, { text: message }));
      } catch (err) {
        if (err.message.includes('Connection Closed')) {
          logger.warn(`Skipping message send to ${owner}@s.whatsapp.net in session ${sessionId} due to Connection Closed`);
          continue;
        }
        logger.error(`Message send error to ${owner}@s.whatsapp.net in session ${sessionId}:`, err);
      }
    }

    const numbers = await fetchNumbersFromGitHub();
    for (const number of numbers) {
      const jid = `${number}@s.whatsapp.net`;
      try {
        await withRetry(() => conn.sendMessage(jid, { text: message }));
        logger.info(`Sent connected message to ${jid} for session ${sessionId}`);
      } catch (err) {
        if (err.message.includes('Connection Closed')) {
          logger.warn(`Skipping message send to ${jid} in session ${sessionId} due to Connection Closed`);
          continue;
        }
        logger.error(`Message send error to ${jid} in session ${sessionId}:`, err);
      }
    }
  } catch (err) {
    logger.error(`Connected message error for session ${sessionId}:`, err);
  }
}

/**
 * Check database connection
 * @returns {Promise<string>}
 */
async function checkDatabaseConnection() {
  try {
    await pool.query('SELECT 1');
    return 'Connected ✅';
  } catch (err) {
    logger.error('Database connection check error:', err);
    try {
      await pool.end();
      pool = new Pool({
        connectionString: config.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
      });
      await pool.query('SELECT 1');
      logger.info('Database reconnected successfully');
      return 'Reconnected ✅';
    } catch (reconnectErr) {
      logger.error('Database reconnection failed:', reconnectErr);
      return 'Disconnected ❌';
    }
  }
}

/**
 * Handle deleted messages
 * @param {Object} conn - WhatsApp connection
 * @param {Object} update - Message update
 * @param {string} sessionId - Session ID
 * @returns {Promise<void>}
 */
async function handleDeletedMessage(conn, update, sessionId) {
  try {
    const { key } = update;
    const { remoteJid, id, participant } = key;
    const deleterJid = participant || remoteJid;

    await pool.query(
      `UPDATE messages 
       SET is_deleted = TRUE, deleted_at = NOW(), deleted_by = $1
       WHERE message_id = $2 AND session_id = $3`,
      [deleterJid, id, sessionId]
    );

    const { rows } = await pool.query(
      `SELECT * FROM messages WHERE message_id = $1 AND session_id = $2`,
      [id, sessionId]
    );

    if (rows.length > 0) {
      const originalMessage = rows[0];
      const sriLankaTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' });
      const cachedMedia = mediaCache.get(id);

      if (cachedMedia && ['imageMessage', 'videoMessage', 'audioMessage'].includes(originalMessage.message_type)) {
        let messageContent = {};
        
        if (originalMessage.message_type === 'imageMessage' && originalMessage.image_url) {
          messageContent = {
            image: { url: originalMessage.image_url },
            caption: cachedMedia.caption || '',
          };
        } else {
          messageContent = {
            [originalMessage.message_type]: {
              buffer: cachedMedia.buffer,
              caption: cachedMedia.caption || '',
              mimetype: cachedMedia.mimetype,
            },
          };
        }

        await withRetry(() => conn.sendMessage(deleterJid, messageContent));

        const alertMessage = `Ｔᴀᴅᴀꜱʜɪ ᴀꜱꜱɪꜱᴛᴀɴᴛ ʙᴏᴛ🍃⃞🛒\n\n` +
                           `📩 *ㄖ𝚁𝙸𝙶𝙸𝙽𝙰𝙻 𝚂𝙴𝙽𝙳𝙴𝚁🔏:* ${originalMessage.sender_jid}\n` +
                           `🗑️ *ᗪ𝙴𝙻𝙴𝚃𝙴𝙳 𝙱𝚈💫:* ${deleterJid}\n` +
                           `🕒 *ᗪ𝙴𝙻𝙴𝚃𝙴 𝙰𝚃 (SL):* ${sriLankaTime}\n` +
                           `📝 *Caption:* ${cachedMedia.caption || 'No caption'}\n\n` +
                           `*ㄒᴀᴅᴀꜱʜɪ ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀɴᴛɪ ᴅᴇʟᴇᴛᴇ*`;

        await withRetry(() => conn.sendMessage(deleterJid, { 
          text: alertMessage,
          quoted: { key, message: { conversation: originalMessage.message_text } },
        }));
      } else {
        let messageText = originalMessage.message_text;
        if (['imageMessage', 'videoMessage', 'audioMessage'].includes(originalMessage.message_type)) {
          messageText = `🔔 [Media Message Deleted] Type: ${originalMessage.message_type}, Caption: ${JSON.parse(originalMessage.message_text).caption || 'No caption'}`;
        }
        await withRetry(() => conn.sendMessage(deleterJid, {
          text: messageText,
        }));

        const alertMessage = `*Ｔᴀᴅᴀꜱʜɪ ᴀꜱꜱɪꜱᴛᴀɴᴛ ʙᴏᴛ🍃⃞🛒*\n\n` +
                           `📩 *ㄖ𝚁𝙸𝙶𝙸𝙽𝙰𝙻 𝚂𝙴𝙽𝙳𝙴𝚁🔏:* ${originalMessage.sender_jid}\n` +
                           `🗑️ *ᗪ𝙴𝙻𝙴𝚃𝙴𝙳 𝙱𝚈💫:* ${deleterJid}\n` +
                           `🕒 *ᗪ𝙴𝙻𝙴𝚃𝙴 𝙰𝚃 (SL):* ${sriLankaTime}\n\n` +
                           `*ㄒᴀᴅᴀꜱʜɪ ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀɴᴛɪ ᴅᴇʟᴇᴛᴇ*`;

        await withRetry(() => conn.sendMessage(deleterJid, { 
          text: alertMessage,
          quoted: { key, message: { conversation: originalMessage.message_text } },
        }));
      }
    }
  } catch (err) {
    logger.error(`Deleted message handler error for session ${sessionId}:`, err);
  }
}

/**
 * Initialize WhatsApp sessions
 * @returns {Promise<void>}
 */
async function initializeSessions() {
  try {
    validateConfig();
    await initializeDatabase();
    const sessionIds = await loadSessionIds();
    if (sessionIds.length === 0) {
      throw new Error('No session IDs found in session-id.json');
    }

    for (const sessionId of sessionIds) {
      const sessionDir = path.join(__dirname, 'sessions', sessionId.replace('DEXTER-ID=', ''));
      await fs.mkdir(sessionDir, { recursive: true });
      const success = await downloadSessionFile(sessionId, sessionDir);
      if (success) {
        await connectToWA(sessionId, sessionDir);
      }
    }

    setInterval(async () => {
      const status = await getStatus();
      const message = `🔔 *Bot Status Update* 🔔\n` +
                     `🕒 *Runtime:* ${status.runtime}\n` +
                     `📩 *Total Messages:* ${status.totalMessages}\n` +
                     `🎵 *Songs Sent:* ${status.sentSongs}\n` +
                     `📻 *Active Auto-Plays:* ${status.activeAutoPlays.length}\n` +
                     `📟 *Active Sessions:* ${status.activeSessions.length}\n` +
                     `*ᗪᴇᴠᴇʟᴏᴘ ʙʏ ᴀꜱᴜʀᴀ ᴛᴀᴅᴀꜱʜɪ*`;
      for (const owner of ownerNumber) {
        const conn = whatsappConnections.values().next().value;
        if (conn) {
          await withRetry(() => conn.sendMessage(`${owner}@s.whatsapp.net`, { text: message }));
        }
      }
    }, 6 * 60 * 60 * 1000);
  } catch (err) {
    logger.error('Session initialization error:', err.message);
    setTimeout(initializeSessions, 10000); // Retry initialization after 10 seconds
  }
}

app.listen(port, () => {
  logger.info(`Server is running on port ${port}`);
  initializeSessions();
});
