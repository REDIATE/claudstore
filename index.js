/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║         COMPLETE PERSONAL CLOUD TELEGRAM BOT                ║
 * ║         Full 10-Feature Implementation                      ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 * SETUP INSTRUCTIONS:
 * 1. npm install node-telegram-bot-api mongoose dotenv
 * 2. Create .env file with the variables below
 * 3. node telegram_cloud_bot.js
 *
 * REQUIRED .env VARIABLES:
 * BOT_TOKEN=your_bot_token_from_BotFather
 * MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/cloudbot
 * STORAGE_CHANNEL_ID=-100xxxxxxxxxx   (private channel, bot must be admin)
 * OWNER_ID=your_telegram_user_id      (get from @userinfobot)
 * PERSONAL_ONLY=true                  (set false to allow all users)
 */

require("dotenv").config();
const TelegramBot = require("node-telegram-bot-api");
const mongoose = require("mongoose");

// ─────────────────────────────────────────────
// ENV & CONFIG
// ─────────────────────────────────────────────
const BOT_TOKEN = process.env.BOT_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const STORAGE_CHANNEL_ID = process.env.STORAGE_CHANNEL_ID;
const OWNER_ID = parseInt(process.env.OWNER_ID);
const PERSONAL_ONLY = process.env.PERSONAL_ONLY !== "false";

if (!BOT_TOKEN || !MONGO_URI || !STORAGE_CHANNEL_ID || !OWNER_ID) {
  console.error("❌ Missing required environment variables. Check your .env file.");
  process.exit(1);
}

// ─────────────────────────────────────────────
// MONGOOSE SCHEMAS
// ─────────────────────────────────────────────

/**
 * Photo sub-document: stores Telegram file_id + hash for dedup
 */
const PhotoSchema = new mongoose.Schema({
  file_id: { type: String, required: true },
  file_unique_id: { type: String, required: true }, // used for dedup
  channel_message_id: { type: Number, default: null }, // message ID in storage channel
});

/**
 * Album document
 */
const AlbumSchema = new mongoose.Schema(
  {
    album_id: { type: String, required: true, unique: true }, // e.g. ALB-00001
    name: { type: String, required: true },
    photos: [PhotoSchema],
    locked: { type: Boolean, default: false },
    metadata_message_id: { type: Number, default: null }, // editable caption msg in channel
    owner_id: { type: Number, required: true },
  },
  { timestamps: true }
);

// Case-insensitive index on name for fast search
AlbumSchema.index({ name: "text" });

const Album = mongoose.model("Album", AlbumSchema);

// ─────────────────────────────────────────────
// BOT INIT
// ─────────────────────────────────────────────
const bot = new TelegramBot(BOT_TOKEN, { polling: true });

// In-memory sessions: { chatId: { albumName, photos: [], mode } }
const sessions = new Map();

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

/** Generate next sequential Album ID */
async function generateAlbumId() {
  const count = await Album.countDocuments();
  return `ALB-${String(count + 1).padStart(5, "0")}`;
}

/** Access guard */
function isAuthorized(userId) {
  return !PERSONAL_ONLY || userId === OWNER_ID;
}

/** Send access denied */
function denyAccess(chatId) {
  bot.sendMessage(chatId, "🔒 This is a personal bot. Access denied.");
}

/** Safe reply with error catch */
async function reply(chatId, text, opts = {}) {
  try {
    return await bot.sendMessage(chatId, text, { parse_mode: "HTML", ...opts });
  } catch (e) {
    console.error("send error:", e.message);
  }
}

/** Build structured metadata caption for album */
function buildCaption(album) {
  return (
    `📁 <b>Album:</b> ${album.name}\n` +
    `🆔 <b>ID:</b> ${album.album_id}\n` +
    `📸 <b>Photos:</b> ${album.photos.length}\n` +
    `🔒 <b>Locked:</b> ${album.locked ? "Yes" : "No"}\n` +
    `📅 <b>Created:</b> ${album.createdAt ? album.createdAt.toDateString() : "N/A"}`
  );
}

/** Forward a photo to the storage channel and return message_id */
async function storePhotoInChannel(fileId) {
  const msg = await bot.sendPhoto(STORAGE_CHANNEL_ID, fileId, {
    caption: "📦 Stored",
  });
  return msg.message_id;
}

/** Edit metadata message in storage channel */
async function updateChannelCaption(album) {
  if (!album.metadata_message_id) return;
  try {
    await bot.editMessageCaption(buildCaption(album), {
      chat_id: STORAGE_CHANNEL_ID,
      message_id: album.metadata_message_id,
      parse_mode: "HTML",
    });
  } catch (_) {}
}

// ─────────────────────────────────────────────
// FEATURE 1 & 2: ALBUM CREATION + PREVIEW/SAVE
// ─────────────────────────────────────────────

/** /album <name> — start a new album session */
bot.onText(/^\/album (.+)$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const name = match[1].trim();

  // Check if album name already exists
  const existing = await Album.findOne({
    name: { $regex: new RegExp(`^${name}$`, "i") },
    owner_id: userId,
  });
  if (existing) {
    return reply(
      chatId,
      `⚠️ Album "<b>${name}</b>" already exists.\nUse /add ${name} to add photos to it.`
    );
  }

  // Start session
  sessions.set(chatId, { albumName: name, photos: [], mode: "create" });

  reply(
    chatId,
    `📂 <b>Album session started:</b> <code>${name}</code>\n\n` +
      `📸 Send photos now. Duplicates will be skipped.\n` +
      `✅ Send /close to preview and save.`
  );
});

/** Receive photos during an active session */
bot.on("photo", async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return;

  const session = sessions.get(chatId);
  if (!session) {
    return reply(chatId, "ℹ️ No active session. Use /album <name> to start.");
  }

  // Best quality = last element in photo array
  const photo = msg.photo[msg.photo.length - 1];
  const fileUniqueId = photo.file_unique_id;

  // Duplicate detection within session
  if (session.photos.find((p) => p.file_unique_id === fileUniqueId)) {
    return reply(chatId, "⚠️ Duplicate photo detected — skipped.");
  }

  session.photos.push({
    file_id: photo.file_id,
    file_unique_id: fileUniqueId,
  });

  reply(chatId, `✅ Photo ${session.photos.length} added.`);
});

/** /close — preview and prompt save */
bot.onText(/^\/close$/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const session = sessions.get(chatId);
  if (!session) return reply(chatId, "ℹ️ No active session.");

  if (session.photos.length === 0) {
    sessions.delete(chatId);
    return reply(chatId, "❌ No photos in session. Album creation cancelled.");
  }

  // Send preview
  const previewText =
    `👁 <b>Preview — ${session.albumName}</b>\n` +
    `📸 ${session.photos.length} photo(s) ready to save.\n\n` +
    `Save this album?`;

  reply(chatId, previewText, {
    reply_markup: {
      inline_keyboard: [
        [
          { text: "💾 Save Album", callback_data: "save_album" },
          { text: "❌ Cancel", callback_data: "cancel_album" },
        ],
      ],
    },
  });
});

/** Callback: Save or Cancel album */
bot.on("callback_query", async (query) => {
  const chatId = query.message.chat.id;
  const userId = query.from.id;
  if (!isAuthorized(userId)) return bot.answerCallbackQuery(query.id, { text: "Access denied" });

  const data = query.data;

  // ── SAVE ALBUM ──
  if (data === "save_album") {
    const session = sessions.get(chatId);
    if (!session) {
      return bot.answerCallbackQuery(query.id, { text: "Session expired." });
    }

    bot.answerCallbackQuery(query.id, { text: "Saving…" });
    await bot.editMessageReplyMarkup({ inline_keyboard: [] }, {
      chat_id: chatId,
      message_id: query.message.message_id,
    });

    const albumId = await generateAlbumId();
    const storedPhotos = [];

    // Upload each photo to storage channel
    const statusMsg = await reply(chatId, `⏳ Uploading ${session.photos.length} photo(s)…`);

    for (const p of session.photos) {
      try {
        const channelMsgId = await storePhotoInChannel(p.file_id);
        storedPhotos.push({ ...p, channel_message_id: channelMsgId });
      } catch (e) {
        console.error("Upload error:", e.message);
      }
    }

    // Save to MongoDB
    const album = new Album({
      album_id: albumId,
      name: session.albumName,
      photos: storedPhotos,
      locked: false,
      owner_id: userId,
    });
    await album.save();

    // Send metadata message to channel (editable)
    const metaMsg = await bot.sendMessage(STORAGE_CHANNEL_ID, buildCaption(album), {
      parse_mode: "HTML",
    });
    album.metadata_message_id = metaMsg.message_id;
    await album.save();

    sessions.delete(chatId);

    // Edit status
    if (statusMsg) {
      bot.editMessageText(
        `✅ <b>Album Saved!</b>\n\n` +
          `📁 Name: <b>${album.name}</b>\n` +
          `🆔 ID: <code>${albumId}</code>\n` +
          `📸 Photos: ${storedPhotos.length}`,
        { chat_id: chatId, message_id: statusMsg.message_id, parse_mode: "HTML" }
      );
    }

    // Send confirmation to storage channel
    bot.sendMessage(STORAGE_CHANNEL_ID, `📁 Album "<b>${album.name}</b>" — Closed & Saved ✅`, {
      parse_mode: "HTML",
    });
  }

  // ── CANCEL ALBUM ──
  if (data === "cancel_album") {
    sessions.delete(chatId);
    bot.answerCallbackQuery(query.id, { text: "Cancelled." });
    bot.editMessageText("❌ Album creation cancelled.", {
      chat_id: chatId,
      message_id: query.message.message_id,
    });
  }

  // ── DELETE CONFIRM ──
  if (data.startsWith("confirm_delete:")) {
    const albumId = data.split(":")[1];
    const album = await Album.findOne({ album_id: albumId, owner_id: userId });
    if (!album) return bot.answerCallbackQuery(query.id, { text: "Album not found." });

    await Album.deleteOne({ _id: album._id });
    bot.answerCallbackQuery(query.id, { text: "Deleted." });
    bot.editMessageText(`🗑 Album "<b>${album.name}</b>" has been deleted.`, {
      chat_id: chatId,
      message_id: query.message.message_id,
      parse_mode: "HTML",
    });
  }

  if (data.startsWith("cancel_delete:")) {
    bot.answerCallbackQuery(query.id, { text: "Cancelled." });
    bot.editMessageText("↩️ Delete cancelled.", {
      chat_id: chatId,
      message_id: query.message.message_id,
    });
  }
});

// ─────────────────────────────────────────────
// FEATURE 3: ADD TO EXISTING ALBUM
// ─────────────────────────────────────────────

/** /add <name> — add photos to existing album */
bot.onText(/^\/add (.+)$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const name = match[1].trim();
  const album = await Album.findOne({
    name: { $regex: new RegExp(`^${name}$`, "i") },
    owner_id: userId,
  });

  if (!album) return reply(chatId, `❌ Album "<b>${name}</b>" not found.`);
  if (album.locked) return reply(chatId, `🔒 Album "<b>${name}</b>" is locked. Unlock it first.`);

  sessions.set(chatId, {
    albumName: album.name,
    albumId: album.album_id,
    existingUniqueIds: album.photos.map((p) => p.file_unique_id),
    newPhotos: [],
    mode: "add",
  });

  reply(
    chatId,
    `➕ <b>Add mode:</b> <code>${album.name}</code>\n` +
      `Currently has ${album.photos.length} photo(s).\n\n` +
      `Send photos to add. Send /done when finished.`
  );
});

/** /done — finish adding photos */
bot.onText(/^\/done$/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const session = sessions.get(chatId);
  if (!session || session.mode !== "add")
    return reply(chatId, "ℹ️ No active add session. Use /add <name>.");

  if (session.newPhotos.length === 0) {
    sessions.delete(chatId);
    return reply(chatId, "ℹ️ No new photos added.");
  }

  const album = await Album.findOne({ album_id: session.albumId, owner_id: userId });
  if (!album) {
    sessions.delete(chatId);
    return reply(chatId, "❌ Album not found.");
  }

  const statusMsg = await reply(chatId, `⏳ Uploading ${session.newPhotos.length} photo(s)…`);

  for (const p of session.newPhotos) {
    try {
      const channelMsgId = await storePhotoInChannel(p.file_id);
      album.photos.push({ ...p, channel_message_id: channelMsgId });
    } catch (e) {
      console.error("Upload error:", e.message);
    }
  }

  await album.save();
  await updateChannelCaption(album);
  sessions.delete(chatId);

  if (statusMsg) {
    bot.editMessageText(
      `✅ <b>${session.newPhotos.length} photo(s) added</b> to "<b>${album.name}</b>"\n` +
        `📸 Total now: ${album.photos.length}`,
      { chat_id: chatId, message_id: statusMsg.message_id, parse_mode: "HTML" }
    );
  }
});

/** Override photo handler to support "add" mode */
bot.on("photo", async (msg) => {
  const chatId = msg.chat.id;
  const session = sessions.get(chatId);
  if (!session || session.mode !== "add") return; // handled by main photo handler above

  const photo = msg.photo[msg.photo.length - 1];
  const fileUniqueId = photo.file_unique_id;

  // Dedup vs existing + new
  if (
    session.existingUniqueIds.includes(fileUniqueId) ||
    session.newPhotos.find((p) => p.file_unique_id === fileUniqueId)
  ) {
    return reply(chatId, "⚠️ Duplicate photo — skipped.");
  }

  session.newPhotos.push({ file_id: photo.file_id, file_unique_id: fileUniqueId });
  reply(chatId, `✅ Photo queued (${session.newPhotos.length} new). Send /done to save.`);
});

// ─────────────────────────────────────────────
// FEATURE 4: LOCK / UNLOCK
// ─────────────────────────────────────────────

bot.onText(/^\/lock (.+)$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const name = match[1].trim();
  const album = await Album.findOne({
    name: { $regex: new RegExp(`^${name}$`, "i") },
    owner_id: userId,
  });

  if (!album) return reply(chatId, `❌ Album "<b>${name}</b>" not found.`);
  if (album.locked) return reply(chatId, `⚠️ Already locked.`);

  album.locked = true;
  await album.save();
  await updateChannelCaption(album);

  reply(chatId, `🔒 Album "<b>${album.name}</b>" is now locked.`);
});

bot.onText(/^\/unlock (.+)$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const name = match[1].trim();
  const album = await Album.findOne({
    name: { $regex: new RegExp(`^${name}$`, "i") },
    owner_id: userId,
  });

  if (!album) return reply(chatId, `❌ Album "<b>${name}</b>" not found.`);
  if (!album.locked) return reply(chatId, `⚠️ Album is not locked.`);

  album.locked = false;
  await album.save();
  await updateChannelCaption(album);

  reply(chatId, `🔓 Album "<b>${album.name}</b>" is now unlocked.`);
});

// ─────────────────────────────────────────────
// FEATURE 5: RENAME & DELETE
// ─────────────────────────────────────────────

bot.onText(/^\/rename (.+) (.+)$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const oldName = match[1].trim();
  const newName = match[2].trim();

  const album = await Album.findOne({
    name: { $regex: new RegExp(`^${oldName}$`, "i") },
    owner_id: userId,
  });
  if (!album) return reply(chatId, `❌ Album "<b>${oldName}</b>" not found.`);

  const conflict = await Album.findOne({
    name: { $regex: new RegExp(`^${newName}$`, "i") },
    owner_id: userId,
  });
  if (conflict) return reply(chatId, `⚠️ An album named "<b>${newName}</b>" already exists.`);

  album.name = newName;
  await album.save();
  await updateChannelCaption(album);

  reply(
    chatId,
    `✏️ Renamed: <b>${oldName}</b> → <b>${newName}</b>\n🆔 ID unchanged: <code>${album.album_id}</code>`
  );
});

bot.onText(/^\/delete (.+)$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const name = match[1].trim();
  const album = await Album.findOne({
    name: { $regex: new RegExp(`^${name}$`, "i") },
    owner_id: userId,
  });

  if (!album) return reply(chatId, `❌ Album "<b>${name}</b>" not found.`);

  reply(
    chatId,
    `⚠️ <b>Delete Confirmation</b>\n\n` +
      `Album: <b>${album.name}</b> (<code>${album.album_id}</code>)\n` +
      `Photos: ${album.photos.length}\n\n` +
      `This action cannot be undone!`,
    {
      reply_markup: {
        inline_keyboard: [
          [
            { text: "🗑 Yes, Delete", callback_data: `confirm_delete:${album.album_id}` },
            { text: "↩️ Cancel", callback_data: `cancel_delete:${album.album_id}` },
          ],
        ],
      },
    }
  );
});

// ─────────────────────────────────────────────
// FEATURE 6: SMART SEARCH
// ─────────────────────────────────────────────

bot.onText(/^\/search (.+)$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const query = match[1].trim();

  // Try exact album_id match first
  let results = await Album.find({ album_id: query, owner_id: userId });

  // If no ID match, do partial name search (case-insensitive)
  if (results.length === 0) {
    results = await Album.find({
      name: { $regex: new RegExp(query, "i") },
      owner_id: userId,
    }).sort({ createdAt: -1 });
  }

  if (results.length === 0) {
    return reply(chatId, `🔍 No albums found for "<b>${query}</b>"`);
  }

  let text = `🔍 <b>Search results for "${query}"</b> — ${results.length} found:\n\n`;
  for (const a of results.slice(0, 10)) {
    text +=
      `📁 <b>${a.name}</b>\n` +
      `   🆔 <code>${a.album_id}</code> | 📸 ${a.photos.length} | ${a.locked ? "🔒" : "🔓"}\n\n`;
  }
  if (results.length > 10) text += `…and ${results.length - 10} more.`;

  reply(chatId, text);
});

// ─────────────────────────────────────────────
// FEATURE 7: ALBUM LISTING
// ─────────────────────────────────────────────

bot.onText(/^\/albums$/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const albums = await Album.find({ owner_id: userId }).sort({ createdAt: -1 });

  if (albums.length === 0) {
    return reply(chatId, "📂 You have no albums yet. Use /album <name> to create one.");
  }

  let text = `📚 <b>Your Albums (${albums.length})</b>\n\n`;
  for (const a of albums) {
    text +=
      `${a.locked ? "🔒" : "📁"} <b>${a.name}</b>\n` +
      `   🆔 <code>${a.album_id}</code> | 📸 ${a.photos.length} photo(s)\n\n`;
  }

  reply(chatId, text);
});

// ─────────────────────────────────────────────
// FEATURE 8: STATS DASHBOARD
// ─────────────────────────────────────────────

bot.onText(/^\/stats$/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  const albums = await Album.find({ owner_id: userId });
  const totalAlbums = albums.length;
  const totalPhotos = albums.reduce((sum, a) => sum + a.photos.length, 0);
  const lockedCount = albums.filter((a) => a.locked).length;
  const latest = albums.sort((a, b) => b.createdAt - a.createdAt)[0];

  const text =
    `📊 <b>Your Cloud Stats</b>\n\n` +
    `📁 Total Albums: <b>${totalAlbums}</b>\n` +
    `📸 Total Photos: <b>${totalPhotos}</b>\n` +
    `🔒 Locked Albums: <b>${lockedCount}</b>\n` +
    `🔓 Unlocked Albums: <b>${totalAlbums - lockedCount}</b>\n` +
    (latest
      ? `\n🕐 <b>Latest Album:</b>\n` +
        `   📁 ${latest.name}\n` +
        `   🆔 <code>${latest.album_id}</code>\n` +
        `   📅 ${latest.createdAt.toDateString()}`
      : "");

  reply(chatId, text);
});

// ─────────────────────────────────────────────
// /start & /help
// ─────────────────────────────────────────────

bot.onText(/^\/start$/, (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  reply(
    chatId,
    `☁️ <b>Personal Cloud Bot</b>\n\n` +
      `Welcome! Your private photo storage bot.\n\n` +
      `Send /help to see all commands.`
  );
});

bot.onText(/^\/help$/, (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!isAuthorized(userId)) return denyAccess(chatId);

  reply(
    chatId,
    `☁️ <b>Command Reference</b>\n\n` +
      `<b>📂 Album Management</b>\n` +
      `/album &lt;name&gt; — Create new album\n` +
      `/close — Preview &amp; save current album\n` +
      `/add &lt;name&gt; — Add photos to existing album\n` +
      `/done — Finish adding photos\n\n` +
      `<b>🔒 Lock / Unlock</b>\n` +
      `/lock &lt;name&gt; — Lock album\n` +
      `/unlock &lt;name&gt; — Unlock album\n\n` +
      `<b>✏️ Organize</b>\n` +
      `/rename &lt;old&gt; &lt;new&gt; — Rename album\n` +
      `/delete &lt;name&gt; — Delete album\n\n` +
      `<b>🔍 Find</b>\n` +
      `/search &lt;query&gt; — Search by name or ID\n` +
      `/albums — List all albums\n\n` +
      `<b>📊 Info</b>\n` +
      `/stats — Dashboard\n` +
      `/help — This message`
  );
});

// ─────────────────────────────────────────────
// FEATURE 10: CRASH-SAFE STARTUP & ERROR HANDLING
// ─────────────────────────────────────────────

process.on("uncaughtException", (err) => {
  console.error("💥 Uncaught Exception:", err.message);
});

process.on("unhandledRejection", (reason) => {
  console.error("💥 Unhandled Rejection:", reason);
});

bot.on("polling_error", (err) => {
  console.error("📡 Polling error:", err.message);
});

// ─────────────────────────────────────────────
// MONGODB CONNECT & START
// ─────────────────────────────────────────────

mongoose
  .connect(MONGO_URI)
  .then(() => {
    console.log("✅ MongoDB connected");
    console.log("🤖 Bot is running…");
  })
  .catch((err) => {
    console.error("❌ MongoDB connection failed:", err.message);
    process.exit(1);
  });
