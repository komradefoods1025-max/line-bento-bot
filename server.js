const express = require('express');
const crypto = require('crypto');

const app = express();

const PORT = process.env.PORT || 10000;
const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET || '';
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';

app.get('/', (_req, res) => {
  res.status(200).send('ok');
});

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true });
});

app.post('/webhook', express.raw({ type: '*/*' }), async (req, res) => {
  const rawBody = req.body instanceof Buffer ? req.body : Buffer.from('');
  const signature = req.get('x-line-signature') || '';

  if (!verifySignature(rawBody, signature, CHANNEL_SECRET)) {
    return res.sendStatus(401);
  }

  let body;
  try {
    body = JSON.parse(rawBody.toString('utf8'));
  } catch (err) {
    console.error('JSON parse error:', err);
    return res.sendStatus(400);
  }

  const events = Array.isArray(body.events) ? body.events : [];

  if (events.length === 0) {
    return res.sendStatus(200);
  }

  try {
    for (const event of events) {
      await handleEvent(event);
    }
    return res.sendStatus(200);
  } catch (err) {
    console.error('handleEvent error:', err);
    return res.sendStatus(500);
  }
});

async function handleEvent(event) {
  if (!event.replyToken) return;

  if (event.type === 'message' && event.message?.type === 'text') {
    const text = (event.message.text || '').trim();

    if (['予約', '弁当予約', 'ランチ弁当予約'].includes(text)) {
      await replyMessage(event.replyToken, [
        {
          type: 'text',
          text: '接続OKです。次に予約フローを入れていきます。'
        }
      ]);
      return;
    }

    await replyMessage(event.replyToken, [
      {
        type: 'text',
        text: '予約するときは「予約」と送ってください。'
      }
    ]);
  }
}

function verifySignature(rawBody, signature, secret) {
  if (!secret || !signature) return false;

  const hash = crypto
    .createHmac('SHA256', secret)
    .update(rawBody)
    .digest('base64');

  try {
    return crypto.timingSafeEqual(
      Buffer.from(hash),
      Buffer.from(signature)
    );
  } catch {
    return false;
  }
}

async function replyMessage(replyToken, messages) {
  if (!CHANNEL_ACCESS_TOKEN) {
    throw new Error('LINE_CHANNEL_ACCESS_TOKEN が未設定です');
  }

  const response = await fetch('https://api.line.me/v2/bot/message/reply', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({
      replyToken,
      messages
    })
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`Reply API error: ${response.status} ${text}`);
  }
}

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});
