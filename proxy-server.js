/**
 * AI API 限流代理服务器
 * 
 * 作用：在本地启动一个代理，接管 TraeCode 发来的 AI API 请求，
 *       自动处理额度限流（429 错误），排队等待后重试。
 * 
 * 使用方式：
 *   1. 终端运行：node proxy-server.js
 *   2. 在 TraeCode 设置 > 模型 > 添加自定义模型中，
 *      配置 API 地址为 http://localhost:13000
 * 
 * 依赖：Node.js 内置，无需安装任何 npm 包
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');

// ===================== 配置区域 =====================
const CONFIG = {
  // 本地代理监听端口
  PROXY_PORT: 13000,

  // 目标 API 地址（你的实际 AI 接口地址）
  TARGET_URL: 'https://dashscope.aliyuncs.com/compatible-mode/v1',

  // 遇到 429 限流时，等待多少秒后重试
  RETRY_AFTER_SECONDS: 60,

  // 最大重试次数（防止死循环）
  MAX_RETRY: 10,

  // 日志配置
  LOG_DIR: 'logs',                // 日志存放目录（相对本文件），留空则不写文件
  LOG_KEEP_DAYS: 0,               // 保留天数，0 或负数表示永久保留、不自动清理

  // 服务存活心跳日志间隔（秒）
  // 作用：日志中若连续多个心跳周期缺失，说明进程已经退出，
  //       用于区分「服务挂了」与「确实没有请求流量」
  HEARTBEAT_LOG_SECONDS: 300,
};
// ====================================================

// ---- 日志工具（按日分文件，自动清理过期日志） ----
const LOG_DIR = CONFIG.LOG_DIR ? path.resolve(__dirname, CONFIG.LOG_DIR) : null;
const LOG_KEEP_MS = CONFIG.LOG_KEEP_DAYS * 24 * 60 * 60 * 1000;
let logStream = null;
let currentLogDate = '';

/** 获取当天日志文件的完整路径 */
function getLogFilePath() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const dateStr = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  return path.join(LOG_DIR, `proxy-server-${dateStr}.log`);
}

/** 确保日志文件流指向当天文件 */
function ensureLogStream() {
  if (!LOG_DIR) return;
  const today = new Date().toISOString().slice(0, 10); // '2026-09-30'
  if (currentLogDate !== today) {
    if (logStream) logStream.end();
    currentLogDate = today;
    // 日志目录可能还不存在（首次运行或刚改过配置），先确保创建
    if (!fs.existsSync(LOG_DIR)) {
      fs.mkdirSync(LOG_DIR, { recursive: true });
    }
    logStream = fs.createWriteStream(getLogFilePath(), { flags: 'a', encoding: 'utf-8' });
  }
}

/** 启动时清理超过保留天数的旧日志（LOG_KEEP_DAYS <= 0 时永久保留） */
function cleanOldLogs() {
  if (!LOG_DIR) return;
  // 0 或负数表示永久保留，不执行清理
  if (CONFIG.LOG_KEEP_DAYS <= 0) return;
  const now = Date.now();
  try {
    const files = fs.readdirSync(LOG_DIR);
    files.forEach(file => {
      if (file.startsWith('proxy-server-') && file.endsWith('.log')) {
        const filePath = path.join(LOG_DIR, file);
        const stat = fs.statSync(filePath);
        if (now - stat.mtimeMs > LOG_KEEP_MS) {
          fs.unlinkSync(filePath);
          console.log(`[日志] 清理过期日志: ${file}`);
        }
      }
    });
  } catch (e) {
    console.error('[日志] 清理过期日志失败:', e.message);
  }
}

/**
 * 格式化时间戳：YYYY-MM-DD HH:MM:SS
 */
function formatTimestamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

/**
 * 统一日志输出：同时写入控制台和日志文件
 * @param {'INFO'|'WARN'|'ERROR'} level - 日志等级
 * @param {string} message - 日志内容
 */
function log(level, message) {
  ensureLogStream();

  const ts = formatTimestamp();
  const line = `[${ts}] [${level}] ${message}`;

  // 控制台输出
  if (level === 'ERROR') {
    console.error(line);
  } else if (level === 'WARN') {
    console.warn(line);
  } else {
    console.log(line);
  }

  // 写入日志文件
  if (logStream) {
    logStream.write(line + '\n');
  }
}
// ====================================================

// ---- 兜底：捕获未捕获异常，记录日志后继续运行 ----
process.on('uncaughtException', (err, origin) => {
  const ts = formatTimestamp();
  const line = `[${ts}] [FATAL] 💥 未捕获异常: ${err.message} | origin: ${origin} | stack: ${err.stack}`;
  console.error(line);
  // 尝试写入日志文件
  if (logStream) {
    logStream.write(line + '\n');
  } else {
    // 兜底：直接追加到今天的日志文件
    try {
      const logFile = getLogFilePath();
      fs.appendFileSync(logFile, line + '\n', 'utf-8');
    } catch (e) {
      console.error('[日志] 无法写入致命错误:', e.message);
    }
  }
});
// ====================================================

// ---- 并发处理（不使用串行队列） ----
// 串行队列会被慢请求阻塞：大上下文请求上游可能耗时 1~2 分钟，
// 期间后续请求全部排队，客户端等不到响应就自行断开（TraeCode 报 4054）。
// 现在每个请求独立并发处理，互不阻塞。
let activeRequests = 0; // 当前正在处理的请求数
let requestCounter = 0; // 自增请求ID，方便关联同一次请求的所有日志

/** 从请求体中提取模型名（可能没有） */
function extractModel(body) {
  if (!body) return '';
  try {
    const parsed = JSON.parse(body);
    return parsed.model || '';
  } catch {
    return '';
  }
}

// ---- 会话识别 ----
// 同一对话的多轮请求只会在消息末尾追加内容，开头的 system 提示词与第一条 user 消息保持不变，
// 因此用它们生成稳定指纹，就能把并行进行的多个对话区分开。
const conversations = new Map(); // 会话ID -> 会话统计信息

/** 把消息 content（可能是字符串或数组）转成纯文本 */
function contentToText(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.map(part => (part && (part.text || '')) || '').join(' ');
  }
  return '';
}

/** 从请求头中探测客户端可能携带的会话标识，取到即优先使用 */
function extractSessionFromHeaders(headers) {
  const candidates = [
    'x-conversation-id', 'x-session-id', 'x-thread-id',
    'x-chat-id', 'conversation-id', 'session-id',
  ];
  for (const key of candidates) {
    const value = headers[key];
    if (value) return String(value).slice(0, 12);
  }
  return '';
}

/**
 * 计算对话指纹：system 提示词 + 第一条 user 消息的 MD5 前 6 位
 * @param {string} body - 请求体 JSON 字符串
 * @returns {string} 会话标识，无法识别时返回 'unknown'
 */
function computeConversationId(body) {
  try {
    const parsed = JSON.parse(body);
    const messages = parsed.messages || [];
    const systemMsg = messages.find(m => m.role === 'system');
    const firstUserMsg = messages.find(m => m.role === 'user');
    const systemText = systemMsg ? contentToText(systemMsg.content) : '';
    const userText = firstUserMsg ? contentToText(firstUserMsg.content) : '';
    // 各截取前 2000 字符：开头足以区分不同对话，同时避免超大文本的哈希开销
    const seed = systemText.slice(0, 2000) + '\n---\n' + userText.slice(0, 2000);
    if (!systemText && !userText) return 'unknown';
    return crypto.createHash('md5').update(seed).digest('hex').slice(0, 6);
  } catch {
    return 'unknown';
  }
}

/**
 * 登记会话并返回会话记录
 * 首次出现时记录开场摘要与 system 提示词片段，方便人工识别是哪个对话
 * @param {string} convId - 会话标识
 * @param {string} body - 请求体 JSON 字符串
 * @returns {object} 会话记录
 */
function registerConversation(convId, body) {
  const now = Date.now();
  const existing = conversations.get(convId);
  if (existing) {
    existing.requests++;
    existing.lastSeen = now;
    return existing;
  }

  let opening = '';
  let systemPreview = '';
  try {
    const parsed = JSON.parse(body);
    const messages = parsed.messages || [];
    const systemMsg = messages.find(m => m.role === 'system');
    const firstUserMsg = messages.find(m => m.role === 'user');
    systemPreview = systemMsg ? contentToText(systemMsg.content).replace(/\s+/g, ' ').slice(0, 40) : '';
    opening = firstUserMsg ? contentToText(firstUserMsg.content).replace(/\s+/g, ' ').slice(0, 60) : '';
  } catch {
    // 解析失败时摘要留空，不影响主流程
  }

  const conv = {
    id: convId,
    isNew: true,
    requests: 1,
    errors: 0,
    opening,
    systemPreview,
    createdAt: now,
    lastSeen: now,
  };
  conversations.set(convId, conv);
  return conv;
}

/** 清理长时间未活跃的会话记录，避免 Map 无限增长（默认 24 小时） */
function cleanupConversations(maxIdleMs = 24 * 60 * 60 * 1000) {
  const expireBefore = Date.now() - maxIdleMs;
  for (const [id, conv] of conversations) {
    if (conv.lastSeen < expireBefore) conversations.delete(id);
  }
}

/** 人类可读的字节大小 */
function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
}

/**
 * 提取请求中的消息统计
 * - textBytes: 纯文本内容的 UTF-8 字节数（真正的对话上下文）
 * - nonTextBytes: 图片等非文本内容（如 base64）的字节数
 * - toolBytes: 工具调用/工具结果的字节数
 * 三者结合可定位「请求体很大但 prompt 很小」的原因
 */
function extractMessageStats(body) {
  if (!body) return { count: 0, textBytes: 0, nonTextBytes: 0, toolBytes: 0 };
  try {
    const parsed = JSON.parse(body);
    const messages = parsed.messages || [];
    let textBytes = 0;
    let nonTextBytes = 0;
    let toolBytes = 0;

    messages.forEach(msg => {
      if (typeof msg.content === 'string') {
        textBytes += Buffer.byteLength(msg.content, 'utf-8');
      } else if (Array.isArray(msg.content)) {
        msg.content.forEach(part => {
          if (part && part.text) {
            textBytes += Buffer.byteLength(part.text, 'utf-8');
          } else {
            // 图片等非文本片段，按序列化后的字节数统计
            nonTextBytes += Buffer.byteLength(JSON.stringify(part), 'utf-8');
          }
        });
      }
      if (msg.tool_calls) {
        toolBytes += Buffer.byteLength(JSON.stringify(msg.tool_calls), 'utf-8');
      }
    });

    return { count: messages.length, textBytes, nonTextBytes, toolBytes };
  } catch {
    return { count: 0, textBytes: 0, nonTextBytes: 0, toolBytes: 0 };
  }
}

/** 提取上游响应中与限流相关的头信息 */
function extractRateLimitInfo(headers) {
  const parts = [];
  if (headers['x-ratelimit-limit']) parts.push(`limit=${headers['x-ratelimit-limit']}`);
  if (headers['x-ratelimit-remaining']) parts.push(`remaining=${headers['x-ratelimit-remaining']}`);
  if (headers['x-ratelimit-reset']) parts.push(`reset=${headers['x-ratelimit-reset']}`);
  if (headers['x-request-id']) parts.push(`reqId=${headers['x-request-id']}`);
  return parts.length ? parts.join(' ') : '';
}

/**
 * 读取上游响应体全部内容，并根据 content-encoding 自动解压
 * 上游（如 DashScope）返回错误响应时也可能使用 gzip 压缩，
 * 若不先解压直接 toString 会得到乱码，导致无法定位真实错误信息。
 * @param {object} proxyRes - 上游响应对象
 * @returns {Promise<string>} 解压后的响应体文本
 */
function readResponseBody(proxyRes) {
  return new Promise((resolve) => {
    const chunks = [];
    proxyRes.on('data', c => chunks.push(c));
    proxyRes.on('end', () => {
      const buf = Buffer.concat(chunks);
      const encoding = (proxyRes.headers['content-encoding'] || '').toLowerCase();
      try {
        if (encoding === 'gzip') return resolve(zlib.gunzipSync(buf).toString('utf-8'));
        if (encoding === 'deflate') return resolve(zlib.inflateSync(buf).toString('utf-8'));
        if (encoding === 'br') return resolve(zlib.brotliDecompressSync(buf).toString('utf-8'));
      } catch (e) {
        // 解压失败则回退为原始内容，避免影响主流程
      }
      resolve(buf.toString('utf-8'));
    });
    proxyRes.on('error', () => resolve(''));
  });
}

/**
 * 确保已向客户端写出 SSE 响应头，并在重试期间发送注释心跳保活
 * 目的：限流/网络错误重试时耗时可能长达数分钟，提前建好 SSE 连接并持续
 *       发送心跳，避免客户端（TraeCode）因长时间无数据而超时断开。
 * 注意：心跳只使用 SSE 注释行（":" 开头），客户端会忽略，绝不产生 content，
 *       因此不会污染 AI 回复内容，也不会干扰 tool_calls 轮次的解析。
 * @param {object} item - 请求项
 * @returns {boolean} 客户端连接是否仍然可用
 */
function ensureSseKeepAlive(item) {
  const { reqId, convId, res, isStream } = item;

  // 客户端已断开，无需保活
  if (res.destroyed || res.writableEnded) return false;

  // 非流式请求无法提前返回响应头，只能原样等待
  if (!isStream) return true;

  if (!item.responseStarted) {
    log('INFO', `[#${reqId}][会话 ${convId}] 📡 提前返回 SSE 响应头（重试中，保持连接）`);
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    });
    res.write(': proxy keep-alive\n\n'); // SSE 注释行心跳，客户端会忽略
    item.responseStarted = true;
  } else {
    // 已建过连接，补一条心跳续命
    res.write(': proxy keep-alive\n\n');
  }
  return true;
}

/**
 * 向客户端返回错误响应，自动适配「已提前建立 SSE 连接」的场景
 * @param {object} item - 请求项
 * @param {number} statusCode - HTTP 状态码（未建 SSE 连接时使用）
 * @param {object} payload - 标准错误体，形如 { error: { message, type } }
 */
function respondError(item, statusCode, payload) {
  const { res, conv } = item;

  // 累加会话错误次数，便于后续汇总时定位出问题的对话
  if (conv) conv.errors++;

  if (item.responseStarted) {
    // 已经写出 SSE 响应头，只能通过 SSE 事件告知错误，不能再 writeHead
    if (!res.destroyed && !res.writableEnded) {
      try {
        res.write(`data: ${JSON.stringify({
          choices: [{
            delta: { role: 'assistant', content: `❌ ${payload.error.message}` },
            index: 0,
            finish_reason: 'stop',
          }]
        })}\n\n`);
        res.write('data: [DONE]\n\n');
        res.end();
      } catch (e) {
        // 连接可能已断开，忽略写入异常
      }
    }
    return;
  }

  if (!res.destroyed && !res.headersSent) {
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(payload));
    return;
  }

  if (!res.destroyed) res.end();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * 带 SSE 注释心跳的等待（限流重试期间保活，防止客户端长时间无数据而超时）
 * 心跳统一使用 SSE 注释行（以 ":" 开头），客户端会忽略，绝不产生 content，
 * 因此不会污染 AI 回复内容，也不会干扰 tool_calls 轮次的解析。
 * @param {number} ms - 等待总时长（毫秒）
 * @param {object} item - 请求项，需包含 res 与 responseStarted 字段
 */
async function sleepWithHeartbeat(ms, item) {
  const HEARTBEAT_INTERVAL = 10000; // 每 10 秒发送一次心跳
  let remaining = ms;
  while (remaining > 0) {
    const chunk = Math.min(HEARTBEAT_INTERVAL, remaining);
    await sleep(chunk);
    remaining -= chunk;

    // 客户端已断开则提前结束等待
    if (item.res.destroyed || item.res.writableEnded) return;

    // 仅在已建立 SSE 连接时发送注释心跳
    if (item.responseStarted) {
      item.res.write(': proxy keep-alive\n\n');
    }
  }
}

/**
 * 发起 HTTP/HTTPS 请求（支持流式和非流式）
 */
function forwardRequest(targetUrl, method, headers, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(targetUrl);
    const mod = url.protocol === 'https:' ? https : http;

    // 构建转发请求头，去掉 host 和 connection 相关
    const forwardHeaders = { ...headers };
    delete forwardHeaders['host'];
    delete forwardHeaders['connection'];
    delete forwardHeaders['content-length'];

    const options = {
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + url.search,
      method: method,
      headers: forwardHeaders,
      timeout: 300000, // 5 分钟超时
    };

    const proxyReq = mod.request(options, (proxyRes) => {
      resolve(proxyRes);
    });

    proxyReq.on('error', (err) => {
      reject(err);
    });

    proxyReq.on('timeout', () => {
      proxyReq.destroy();
      reject(new Error('Request timeout'));
    });

    if (body) {
      proxyReq.write(body);
    }
    proxyReq.end();
  });
}

/**
 * 处理单个请求（含重试逻辑）
 * 每次重试先等待 RETRY_AFTER_SECONDS 秒
 */
async function processRequest(item) {
  const { reqId, convId, conv, req, res, body, model, isStream, enqueuedAt, retryCount } = item;
  const startTime = Date.now();

  // 统一日志前缀：请求编号 + 会话标识，便于在并行对话中定位是哪个对话出的问题
  const tag = `[#${reqId}][会话 ${convId}]`;

  // 并发模式下请求接收完成即开始处理，这里仅记录极短的调度间隔
  // （重试时不重复计算，避免把上一次请求的处理耗时误当等待时间）
  const waitTime = (retryCount === 0 && enqueuedAt) ? startTime - enqueuedAt : 0;

  // 开始处理前检查客户端是否已断开（例如请求体刚发完就取消），
  // 已断开则直接跳过，避免做无意义的转发、白白消耗上游额度
  if (retryCount === 0 && (res.destroyed || res.writableEnded)) {
    log('WARN', `${tag} ⛔ 客户端已断开，跳过本次请求`);
    return;
  }

  // 如果已经在重试中，先等待
  if (retryCount > 0) {
    // 客户端（TraeCode）已断开则直接终止，避免为空连接继续重试
    if (res.destroyed || res.writableEnded) {
      log('WARN', `${tag} ⛔ 客户端已断开，取消重试`);
      return;
    }
    log('WARN', `${tag} ⏳ 等待 ${CONFIG.RETRY_AFTER_SECONDS}s 后重试... (第${retryCount}次)`);
    // 带注释心跳的等待，避免客户端长时间无数据而超时
    await sleepWithHeartbeat(CONFIG.RETRY_AFTER_SECONDS * 1000, item);
  }

  // TARGET_URL 已包含 /v1 路径（如 dashscope.aliyuncs.com/compatible-mode/v1）
  // req.url 也以 /v1/... 开头，去掉 /v1 前缀避免路径重复
  const apiPath = req.url.replace(/^\/v1(\/|$)/, '/');
  const targetUrl = CONFIG.TARGET_URL.replace(/\/+$/, '') + apiPath;
  // TraeCode 配置自定义模型时会自带 Authorization，直接透传
  const headers = req.headers;

  try {
    // 日志：处理延迟 + 开始转发
    if (waitTime > 0) {
      log('INFO', `${tag} 📋 处理延迟: ${waitTime}ms`);
    }
    log('INFO', `${tag} ➡️ 转发 → ${targetUrl}`);

    // 转发请求
    const proxyRes = await forwardRequest(targetUrl, req.method, headers, body);
    const upstreamTime = Date.now() - startTime;

    // === 提取上游响应信息 ===
    const rateLimitInfo = extractRateLimitInfo(proxyRes.headers);
    const statusCode = proxyRes.statusCode;

    // 检查是否 429（额度限流）
    if (statusCode === 429) {
      // 读取上游错误体
      const errBody = await readResponseBody(proxyRes);
      log('WARN', `${tag} 🔄 限流429` +
        (rateLimitInfo ? ` | ${rateLimitInfo}` : '') +
        ` | 上游响应: ${errBody.substring(0, 500)}`);

      // 如果是流式请求，提前返回 SSE 响应头并持续心跳，保持连接不断
      // （避免 TraeCode 在长时间重试等待中主动超时断开）
      ensureSseKeepAlive(item);

      // 检查重试次数
      if (retryCount + 1 > CONFIG.MAX_RETRY) {
        log('ERROR', `${tag} ❌ 超过最大重试次数(${CONFIG.MAX_RETRY})，放弃`);
        respondError(item, 429, {
          error: { message: '超过最大重试次数，上游 API 持续限流', type: 'proxy_rate_limit' }
        });
        return;
      }

      // 客户端已断开则停止重试，避免无意义的"幽灵请求"（白白消耗上游额度）
      if (res.destroyed || res.writableEnded) {
        log('WARN', `${tag} ⛔ 客户端已断开，取消重试`);
        return;
      }

      log('WARN', `${tag} 🔄 等待 ${CONFIG.RETRY_AFTER_SECONDS}s 后重试 (第${retryCount + 1}/${CONFIG.MAX_RETRY}次)`);
      item.retryCount = retryCount + 1;
      await processRequest(item);
      return;
    }

    // 检查是否其他服务端错误（5xx），也适当重试
    if (statusCode >= 500 && statusCode < 600) {
      // 读取上游错误体
      const errBody = await readResponseBody(proxyRes);
      log('WARN', `${tag} 🔄 服务端错误 ${statusCode}` +
        (rateLimitInfo ? ` | ${rateLimitInfo}` : '') +
        ` | 上游响应: ${errBody.substring(0, 500)}`);

      // 5xx 重试同样需要保活，避免客户端等待期间超时断开
      ensureSseKeepAlive(item);

      if (retryCount + 1 > CONFIG.MAX_RETRY) {
        log('WARN', `${tag} ⛔ 超过重试上限，透传 5xx 给客户端`);
        const errBodyBytes = Buffer.byteLength(errBody, 'utf-8');
        if (item.responseStarted) {
          // 已提前建立 SSE 连接，无法再改状态码，只能通过 SSE 告知错误
          respondError(item, statusCode, {
            error: { message: `上游服务端错误 ${statusCode}`, type: 'proxy_upstream_error' }
          });
        } else {
          res.writeHead(statusCode, { 'Content-Type': 'application/json' });
          res.end(errBody);
        }
        log('INFO', `${tag} ✅ 透传 ${statusCode} | body: ${formatBytes(errBodyBytes)}`);
        return;
      }

      // 客户端已断开则停止重试
      if (res.destroyed || res.writableEnded) {
        log('WARN', `${tag} ⛔ 客户端已断开，取消重试`);
        return;
      }

      log('WARN', `${tag} 🔄 等待 ${CONFIG.RETRY_AFTER_SECONDS}s 后重试 (第${retryCount + 1}/${CONFIG.MAX_RETRY}次)`);
      item.retryCount = retryCount + 1;
      await processRequest(item);
      return;
    }

    // === 正常响应（2xx 或 4xx 非 429）===
    // 记录上游响应头（排除内容体相关的，便于排查）
    const headerDebug = Object.keys(proxyRes.headers)
      .filter(k => !['transfer-encoding', 'content-encoding', 'content-length'].includes(k))
      .map(k => `${k}: ${proxyRes.headers[k]}`)
      .join(', ');
    let upstreamLog = `${tag} ⬅️ 上游响应: ${statusCode} (${upstreamTime}ms)`;
    if (rateLimitInfo) upstreamLog += ` | ${rateLimitInfo}`;
    // 记录关键响应头，方便调试编码等问题
    if (proxyRes.headers['content-encoding']) upstreamLog += ` | content-encoding: ${proxyRes.headers['content-encoding']}`;
    log('INFO', upstreamLog);

    // 4xx 非 429：不重试，直接透传
    if (statusCode >= 400 && statusCode < 500) {
      const errBody = await readResponseBody(proxyRes);
      log('WARN', `${tag} ⚠️ 上游返回 ${statusCode}: ${errBody.substring(0, 500)}`);
      const errBodyBytes = Buffer.byteLength(errBody, 'utf-8');
      res.writeHead(statusCode, { 'Content-Type': 'application/json' });
      res.end(errBody);
      log('INFO', `${tag} ✅ 透传 ${statusCode} | body: ${formatBytes(errBodyBytes)} | total: ${Date.now() - startTime}ms`);
      return;
    }

    // === 正常 2xx 响应：转发给客户端 ===
    // 如果已通过 keepalive 发过响应头，跳过 writeHead
    if (!item.responseStarted) {
      const responseHeaders = { ...proxyRes.headers };
      res.writeHead(statusCode, responseHeaders);
    } else {
      log('INFO', `${tag} 📡 SSE 连接已建立，透传上游数据`);
    }

    if (isStream) {
      // 流式响应
      let streamBytes = 0;

      // 采样响应开头 2KB：客户端异常断开时输出，便于确认上游到底返回了什么
      const PREVIEW_LIMIT = 2048;
      res.proxyPreviewChunks = [];
      res.proxyPreviewEncoded = !!proxyRes.headers['content-encoding'];
      let previewBytes = 0;

      // 尾部采样：滚动保留最后 2KB，用于判断客户端断开前是否已收到结束标志
      res.proxyTailChunks = [];
      let tailBytes = 0;

      proxyRes.on('error', (err) => {
        // 如果客户端（res）已断开，说明是客户端超时/取消导致的连锁错误，不重复报错
        if (res.destroyed) return;
        log('ERROR', `${tag} ❌ 流式上游传输错误: ${err.message}`);
        proxyRes.unpipe(res);
        proxyRes.destroy();

        // 上游中途中断时必须给客户端一个明确的结束信号：
        // 否则 SSE 客户端收不到 [DONE] 或错误事件，会一直停在「生成中」状态。
        if (!res.writableEnded) {
          try {
            if (!res.headersSent) {
              // 还没写出响应头，直接返回标准 JSON 错误
              res.writeHead(502, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({
                error: {
                  message: `上游响应中断: ${err.message}`,
                  type: 'proxy_stream_aborted',
                }
              }));
              return;
            }
            // 已建立 SSE 连接：补发错误事件 + 结束标记
            res.write(`data: ${JSON.stringify({
              error: {
                message: `上游响应中断(${err.message})，请重试`,
                type: 'proxy_stream_aborted',
              }
            })}\n\n`);
            res.write('data: [DONE]\n\n');
            res.end();
          } catch (writeErr) {
            // 写入失败说明连接已不可用，销毁兜底
            if (!res.destroyed) res.destroy();
          }
        } else if (!res.destroyed) {
          res.destroy();
        }
      });

      // 客户端断连：清理上游连接
      // （断开本身由 server 层的 res close 统一记录日志，此处不重复打印）
      res.on('error', () => { proxyRes.unpipe(res); proxyRes.destroy(); });
      res.on('close', () => { proxyRes.unpipe(res); proxyRes.destroy(); });

      // 追踪已发送字节数，并采样响应开头与结尾
      proxyRes.on('data', (chunk) => {
        streamBytes += chunk.length;
        res.proxySentBytes = streamBytes;

        // 头部采样（前 2KB）
        if (previewBytes < PREVIEW_LIMIT) {
          const take = Math.min(PREVIEW_LIMIT - previewBytes, chunk.length);
          res.proxyPreviewChunks.push(Buffer.from(chunk.subarray(0, take)));
          previewBytes += take;
        }

        // 尾部采样（滚动窗口，只保留最后 2KB）
        res.proxyTailChunks.push(Buffer.from(chunk));
        tailBytes += chunk.length;
        while (tailBytes > PREVIEW_LIMIT && res.proxyTailChunks.length > 0) {
          const first = res.proxyTailChunks[0];
          if (tailBytes - first.length >= PREVIEW_LIMIT) {
            res.proxyTailChunks.shift();
            tailBytes -= first.length;
          } else {
            // 首块只需要裁掉超出的部分
            const cut = tailBytes - PREVIEW_LIMIT;
            res.proxyTailChunks[0] = first.subarray(cut);
            tailBytes -= cut;
            break;
          }
        }
      });

      proxyRes.on('end', () => {
        // 上游数据已完整接收：此后连接关闭属于正常收尾，
        // 不能当成「客户端提前断开」（否则会误报，干扰 4054 排查）
        res.proxyStreamDone = true;
        if (res.destroyed) return; // 客户端已断开，无需记录
        const totalTime = Date.now() - startTime;
        log('INFO', `${tag} 🏁 流式传输完成 | ` +
          `size: ~${formatBytes(streamBytes)} | total: ${totalTime}ms` +
          (model ? ` | model: ${model}` : ''));
      });

      proxyRes.pipe(res);
    } else {
      // 非流式
      const chunks = [];

      proxyRes.on('error', (err) => {
        log('ERROR', `${tag} ❌ 非流式响应接收错误: ${err.message}`);
        if (!res.destroyed) {
          if (!res.headersSent) {
            res.writeHead(502, { 'Content-Type': 'application/json' });
          }
          res.end(JSON.stringify({ error: { message: `Proxy: ${err.message}`, type: 'proxy_stream_error' } }));
        }
      });

      proxyRes.on('data', chunk => chunks.push(chunk));

      proxyRes.on('end', () => {
        if (!res.destroyed) {
          const responseData = Buffer.concat(chunks);
          res.end(responseData);
          // 响应已完整写出，标记完成，避免 close 时误判为提前断开
          res.proxyStreamDone = true;
          res.proxySentBytes = responseData.length;
          const totalTime = Date.now() - startTime;
          log('INFO', `${tag} ✅ 完成: ${statusCode} | ` +
            `body: ${formatBytes(responseData.length)} | total: ${totalTime}ms` +
            (model ? ` | model: ${model}` : ''));
        }
      });
    }

  } catch (err) {
    const elapsed = Date.now() - startTime;
    log('ERROR', `${tag} ❌ 请求失败 (${elapsed}ms): ${err.message} | stack: ${err.stack?.substring(0, 300)}`);
    // 网络错误也重试
    if (retryCount + 1 <= CONFIG.MAX_RETRY) {
      // 网络错误（如 ECONNRESET）重试耗时可能很长，提前建立 SSE 连接并心跳保活，
      // 否则客户端会先一步超时断开
      ensureSseKeepAlive(item);

      // 客户端已断开则停止重试，避免无意义的"幽灵请求"
      if (res.destroyed || res.writableEnded) {
        log('WARN', `${tag} ⛔ 客户端已断开，取消重试`);
        return;
      }

      log('WARN', `${tag} 🔄 网络错误，${CONFIG.RETRY_AFTER_SECONDS}s 后重试... (第${retryCount + 1}/${CONFIG.MAX_RETRY}次)`);
      item.retryCount = retryCount + 1;
      await processRequest(item);
    } else {
      // 重试次数用尽，按连接状态选择合适的错误返回方式
      respondError(item, 502, {
        error: { message: `Proxy: ${err.message}`, type: 'proxy_network_error' }
      });
    }
  }
}

// ---- 创建 HTTP 服务器 ----
const server = http.createServer((req, res) => {
  // 只要有连接进来就立刻记一条日志（不等请求体收完）
  // 目的：区分「请求根本未到达代理」与「到达后中途断开」，
  //       这是排查 4054（模型请求失败）时最关键的依据
  const peer = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
  log('INFO', `🔌 新连接: ${req.method} ${req.url} | from ${peer} | ` +
    `content-length: ${req.headers['content-length'] || 'chunked'}`);

  // TraeCode 连接中断时主动释放资源
  req.on('error', (err) => {
    log('ERROR', `❌ 客户端请求异常: ${err.message}`);
  });

  // 请求体还没收完就被中断（例如 TraeCode 主动取消、本机网络抖动）
  // 这类情况以前完全不留痕，是排查的盲区
  req.on('aborted', () => {
    log('WARN', `⚠️ 请求体传输被中断: ${req.method} ${req.url}`);
  });
  req.on('close', () => {
    if (!req.complete) {
      log('WARN', `⚠️ 连接已关闭但请求体未接收完整: ${req.method} ${req.url}`);
    }
  });

  // 响应未正常写完就关闭连接（客户端超时/取消），在此统一记录一次，
  // 便于把 TraeCode 报的 4054 与具体请求对上
  res.on('close', () => {
    // 仅当「响应未写完」且「上游数据也未完整接收」时才进入判定流程。
    // 上游已传完（proxyStreamDone）后的连接关闭属于正常收尾，不能误报。
    if (res.writableFinished || res.proxyStreamDone) return;

    const tag = res.proxyReqId
      ? `[#${res.proxyReqId}]${res.proxyConvId ? `[会话 ${res.proxyConvId}]` : ''} `
      : '';
    const sent = res.proxySentBytes || 0;

    // 尾部采样：检查客户端断开前是否已经收到结束标志。
    // 很多 SSE 客户端拿到 finish_reason / [DONE] 后就会主动关闭连接，
    // 这属于正常读完，不应算作「提前断开」。
    let tailText = '';
    if (res.proxyTailChunks && res.proxyTailChunks.length > 0) {
      tailText = Buffer.concat(res.proxyTailChunks).toString('utf-8');
    }
    const clientFinished = /"finish_reason"\s*:\s*"(stop|tool_calls|length|content_filter)"|\[DONE\]/.test(tailText);

    if (clientFinished) {
      log('INFO', `${tag}🟢 客户端已读完回复后关闭连接（已发送 ${formatBytes(sent)}）`);
      return;
    }

    // === 以下为真正的提前断开 ===
    let detail = `已发送: ${formatBytes(sent)}`;

    // 头部采样：查看上游实际返回的内容（压缩内容无法直接阅读，只标注大小）
    if (res.proxyPreviewChunks && res.proxyPreviewChunks.length > 0) {
      if (res.proxyPreviewEncoded) {
        detail += ` | 响应为压缩内容(${formatBytes(Buffer.concat(res.proxyPreviewChunks).length)} 采样)`;
      } else {
        const head = Buffer.concat(res.proxyPreviewChunks)
          .toString('utf-8')
          .replace(/\s+/g, ' ')
          .slice(0, 300);
        if (head) detail += ` | 头部: ${head}`;
      }
    }

    // 尾部采样：看中断发生在哪个事件之后（是否出现 error 等）
    if (tailText) {
      const tail = tailText.replace(/\s+/g, ' ').slice(-300);
      if (tail) detail += ` | 尾部: ${tail}`;
    }

    log('INFO', `${tag}⛔ 客户端提前断开，响应未写完 | ${detail}`);
  });

  // 不设路径过滤，所有请求透传给上游，由上游决定是否 404
  // （TraeCode 发来的请求路径可能是 /v1/chat/completions、/chat/completions 等不同格式）

  // 收集请求体
  const chunks = [];
  req.on('data', chunk => chunks.push(chunk));
  req.on('end', () => {
    const body = Buffer.concat(chunks).toString('utf-8');
    const model = extractModel(body);
    const reqId = ++requestCounter;
    // 优先使用客户端请求头里的会话标识；没有则用「system + 首条 user 消息」生成对话指纹
    const convId = extractSessionFromHeaders(req.headers) || computeConversationId(body);
    const conv = registerConversation(convId, body);

    // 把请求编号与会话标识挂到响应对象上，便于连接级日志（如客户端提前断开）也能带上
    res.proxyReqId = reqId;
    res.proxyConvId = convId;

    const isStream = (body && body.includes('"stream":true')) || req.headers['accept'] === 'text/event-stream';
    const bodyBytes = Buffer.byteLength(body || '', 'utf-8');
    const msgStats = extractMessageStats(body);

    // 先计入并发数，日志里一并展示当前并发情况
    activeRequests++;

    // 新会话首次出现时打印识别线索，方便人工对上是哪个对话
    if (conv.isNew) {
      log('INFO', `🆕 [会话 ${convId}] 新对话 | 开场: ${conv.opening || '(无)'}` +
        (conv.systemPreview ? ` | system: ${conv.systemPreview}` : ''));
      conv.isNew = false;
    }

    // 统一在请求开始处理时记一条详细日志（后续所有日志都带 reqId 与会话标识）
    let reqLog = `↗️ [#${reqId}][会话 ${convId}] ${req.method} ${req.url}` +
      (model ? ` | model: ${model}` : '') +
      ` | body: ${formatBytes(bodyBytes)}` +
      (isStream ? ' | stream' : '') +
      ` | 并发: ${activeRequests}`;
    if (msgStats.count > 0) {
      reqLog += ` | messages: ${msgStats.count}条 | prompt: ${formatBytes(msgStats.textBytes)}`;
      // 请求体远大于 prompt 时，这两个数值能说明多出来的体积来自哪里
      if (msgStats.nonTextBytes > 0) reqLog += ` | 非文本: ${formatBytes(msgStats.nonTextBytes)}`;
      if (msgStats.toolBytes > 0) reqLog += ` | 工具: ${formatBytes(msgStats.toolBytes)}`;
    }
    log('INFO', reqLog);

    const item = {
      reqId,
      convId,
      conv,
      req,
      res,
      body,
      model,
      isStream,
      enqueuedAt: Date.now(),
      retryCount: 0,
      responseStarted: false, // 是否已提前返回 SSE 响应头（限流时保持连接用）
    };

    // 直接并发处理，不排队；捕获异常避免未处理的 Promise 拒绝影响其他请求
    processRequest(item)
      .catch((err) => {
        log('ERROR', `[#${reqId}][会话 ${convId}] 💥 请求处理异常: ${err.message} | stack: ${err.stack?.substring(0, 300)}`);
      })
      .finally(() => {
        activeRequests--;
      });
  });
});

// ---- 启动服务器 ----
server.listen(CONFIG.PROXY_PORT, () => {
  // 清理过期日志 + 打开当天日志流
  cleanOldLogs();
  ensureLogStream();

  log('INFO', '=============================================');
  log('INFO', '  AI API 额度限流代理已启动');
  log('INFO', `  监听地址: http://localhost:${CONFIG.PROXY_PORT}`);
  log('INFO', `  目标 API: ${CONFIG.TARGET_URL}`);
  log('INFO', `  429重试: 等待 ${CONFIG.RETRY_AFTER_SECONDS}s，最多 ${CONFIG.MAX_RETRY} 次`);
  if (LOG_DIR) {
    const keepDesc = CONFIG.LOG_KEEP_DAYS > 0
      ? `保留 ${CONFIG.LOG_KEEP_DAYS} 天`
      : '永久保留';
    log('INFO', `  日志目录: ${LOG_DIR}（按日分文件，${keepDesc}）`);
  }
  log('INFO', '=============================================');
  log('INFO', '');
  log('INFO', '  TraeCode 自定义模型配置提示:');
  log('INFO', `  API 地址   → http://localhost:${CONFIG.PROXY_PORT}`);
  log('INFO', '  API Key 和 模型 ID 在 TraeCode 侧配置，代理直接透传');
  log('INFO', '');
  log('INFO', '  按 Ctrl+C 停止服务器');
  log('INFO', '');

  // 服务存活心跳：每隔一段时间在日志里打一条，
  // 若某天日志中缺少心跳记录，即可判断进程已退出（例如编辑器关闭带走了终端）
  setInterval(() => {
    // 顺带清理 24 小时未活跃的会话记录，避免内存持续增长
    cleanupConversations();
    log('INFO', `💚 服务存活 | 处理中: ${activeRequests} | 活跃会话: ${conversations.size} | 累计请求: ${requestCounter}`);
  }, CONFIG.HEARTBEAT_LOG_SECONDS * 1000).unref();
});

// ---- 服务器级错误监听（如端口被占用），避免静默失败 ----
server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    // 端口被占用通常是重复启动，只记一行，避免堆栈刷屏
    log('FATAL', `💥 端口 ${CONFIG.PROXY_PORT} 已被占用，可能已有另一个代理实例在运行，本进程退出`);
  } else {
    log('FATAL', `💥 服务器启动/运行错误: ${err.message}（端口 ${CONFIG.PROXY_PORT}）| stack: ${err.stack}`);
  }
  // 监听失败时进程无法提供服务，明确退出，交给守护脚本决定是否重启
  process.exit(1);
});

// ---- 进程退出时留下明确日志，便于判断服务为何消失 ----
['SIGINT', 'SIGTERM', 'SIGHUP'].forEach((signal) => {
  process.on(signal, () => {
    log('WARN', `🛑 收到退出信号 ${signal}，服务即将停止`);
    process.exit(0);
  });
});