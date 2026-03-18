const express = require('express');
const crypto = require('crypto');

const app = express();

const PORT = process.env.PORT || 10000;
const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET || '';
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const RESERVATION_SAVE_URL = process.env.RESERVATION_SAVE_URL || '';

const sessions = new Map();
const reservations = [];

const MENUS = {
  karaage: { name: 'からあげ弁当', price: 850 },
  shogayaki: { name: '生姜焼き弁当', price: 900 },
  hamburger: { name: 'ハンバーグ弁当', price: 950 },
  daily: { name: '日替わり弁当', price: 800 }
};

const PICKUP_TIMES = [
  '11:30', '11:45', '12:00', '12:15', '12:30',
  '12:45', '13:00', '13:15', '13:30'
];

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
  const userId = event.source?.userId;
  const replyToken = event.replyToken;

  if (!userId || !replyToken) return;

  if (!sessions.has(userId)) {
    sessions.set(userId, {});
  }
  const session = sessions.get(userId);

  if (event.type === 'follow') {
    clearSession(userId);
    await replyMessage(replyToken, [startGuideMessage()]);
    return;
  }

  if (event.type === 'postback') {
    const data = parsePostbackData(event.postback?.data || '');

    if (data.action === 'reserve_start' || data.action === 'restart') {
      clearSession(userId);
      await replyMessage(replyToken, [buildDatePickerMessage()]);
      return;
    }

    if (data.action === 'pick_date') {
      const selectedDate = event.postback?.params?.date || '';

      if (!selectedDate) {
        await replyMessage(replyToken, [
          textMessage('日付を取得できませんでした。もう一度お試しください。'),
          buildDatePickerMessage()
        ]);
        return;
      }

      if (!isTomorrowOrLater(selectedDate)) {
        await replyMessage(replyToken, [
          textMessage('受取日は翌日以降で選んでください。'),
          buildDatePickerMessage()
        ]);
        return;
      }

      session.date = selectedDate;
      session.step = 'waiting_time';

      await replyMessage(replyToken, [
        textMessage(`受取日：${selectedDate}`),
        buildTimeMessage()
      ]);
      return;
    }

    if (data.action === 'time') {
      const selectedTime = data.value || '';

      if (!PICKUP_TIMES.includes(selectedTime)) {
        await replyMessage(replyToken, [
          textMessage('受取時間をもう一度選んでください。'),
          buildTimeMessage()
        ]);
        return;
      }

      session.time = selectedTime;
      session.step = 'waiting_menu';

      await replyMessage(replyToken, [
        textMessage(`受取時間：${selectedTime}`),
        buildMenuMessage()
      ]);
      return;
    }

    if (data.action === 'menu') {
      const item = data.item || '';
      const menu = MENUS[item];

      if (!menu) {
        await replyMessage(replyToken, [
          textMessage('メニューが見つかりませんでした。もう一度選んでください。'),
          buildMenuMessage()
        ]);
        return;
      }

      session.menuKey = item;
      session.menuName = menu.name;
      session.price = menu.price;
      session.step = 'waiting_qty';

      await replyMessage(replyToken, [
        textMessage(`ご注文商品：${menu.name}`),
        buildQtyMessage(menu.name)
      ]);
      return;
    }

    if (data.action === 'qty') {
      const qty = Number(data.value || 0);

      if (!qty || qty < 1) {
        await replyMessage(replyToken, [
          textMessage('個数をもう一度選んでください。'),
          buildQtyMessage(session.menuName || 'お弁当')
        ]);
        return;
      }

      session.qty = qty;
      session.total = Number(session.price || 0) * qty;
      session.step = 'waiting_name';

      await replyMessage(replyToken, [
        textMessage(`個数：${qty}個`),
        textMessage('ご予約名を入力してください。')
      ]);
      return;
    }

    if (data.action === 'confirm') {
      if (!isReservationComplete(session)) {
        clearSession(userId);
        await replyMessage(replyToken, [
          textMessage('予約情報が不足しています。最初からやり直してください。'),
          startGuideMessage()
        ]);
        return;
      }

      const reservationNo = createReservationNo();
      const reservation = {
        reservationNo,
        userId,
        date: session.date,
        time: session.time,
        menuKey: session.menuKey,
        menuName: session.menuName,
        price: session.price,
        qty: session.qty,
        total: session.total,
        name: session.name,
        phone: session.phone,
        status: '受付済み',
        createdAt: new Date().toISOString()
      };

      const saveResult = await saveReservationToSheet(reservation);

      if (!saveResult.ok) {
        console.error('sheet save error:', saveResult.error);
        await replyMessage(replyToken, [
          textMessage(`予約内容の保存でエラーが起きました。\n${saveResult.error}`)
        ]);
        return;
      }

      reservations.push(reservation);
      console.log('reservation saved:', reservation);

      clearSession(userId);

      await replyMessage(replyToken, [
        textMessage(
          'ご予約ありがとうございます。\n\n' +
          `【予約番号】${reservationNo}\n` +
          `【受取日】${reservation.date}\n` +
          `【受取時間】${reservation.time}\n` +
          `【商品】${reservation.menuName}\n` +
          `【個数】${reservation.qty}個\n` +
          `【合計】¥${reservation.total}\n` +
          `【お名前】${reservation.name}\n` +
          `【電話番号】${reservation.phone}\n` +
          '【お支払い】店頭にてお願いいたします。'
        )
      ]);
      return;
    }
  }

  if (event.type === 'message' && event.message?.type === 'text') {
    const text = (event.message.text || '').trim();

    if (['予約', '弁当予約', 'ランチ弁当予約', '予約したい'].includes(text)) {
      clearSession(userId);
      await replyMessage(replyToken, [buildDatePickerMessage()]);
      return;
    }

    if (['最初から', 'やり直し', 'リセット'].includes(text)) {
      clearSession(userId);
      await replyMessage(replyToken, [buildDatePickerMessage()]);
      return;
    }

    if (session.step === 'waiting_name') {
      session.name = text;
      session.step = 'waiting_phone';

      await replyMessage(replyToken, [
        textMessage(`ご予約名：${text}`),
        textMessage('電話番号を入力してください。\n例：09012345678')
      ]);
      return;
    }

    if (session.step === 'waiting_phone') {
      const phone = normalizePhone(text);

      if (!isValidPhone(phone)) {
        await replyMessage(replyToken, [
          textMessage('電話番号の形式が正しくありません。\n数字のみで入力してください。\n例：09012345678')
        ]);
        return;
      }

      session.phone = phone;
      session.step = 'confirm';

      await replyMessage(replyToken, [
        textMessage(`電話番号：${phone}`),
        buildConfirmMessage(session)
      ]);
      return;
    }

    await replyMessage(replyToken, [startGuideMessage()]);
  }
}

function startGuideMessage() {
  return {
    type: 'text',
    text: 'ランチ弁当のご予約ですね。下のボタンから始めてください。',
    quickReply: {
      items: [
        {
          type: 'action',
          action: {
            type: 'postback',
            label: '予約を始める',
            data: 'action=reserve_start',
            displayText: '予約を始める'
          }
        }
      ]
    }
  };
}

function buildDatePickerMessage() {
  const tomorrow = formatDate(addDays(new Date(), 1));
  const maxDate = formatDate(addDays(new Date(), 30));

  return {
    type: 'text',
    text: '受取日を選んでください。',
    quickReply: {
      items: [
        {
          type: 'action',
          action: {
            type: 'datetimepicker',
            label: '日付を選ぶ',
            data: 'action=pick_date',
            mode: 'date',
            initial: tomorrow,
            min: tomorrow,
            max: maxDate
          }
        }
      ]
    }
  };
}

function buildTimeMessage() {
  return {
    type: 'text',
    text: '受取時間を選んでください。',
    quickReply: {
      items: PICKUP_TIMES.map((time) =>
        quickPostbackItem(time, `action=time&value=${encodeURIComponent(time)}`, time)
      )
    }
  };
}

function buildMenuMessage() {
  return {
    type: 'text',
    text:
      'ご希望のお弁当を選んでください。\n\n' +
      '・からあげ弁当 ¥850\n' +
      '・生姜焼き弁当 ¥900\n' +
      '・ハンバーグ弁当 ¥950\n' +
      '・日替わり弁当 ¥800',
    quickReply: {
      items: [
        quickPostbackItem('からあげ弁当', 'action=menu&item=karaage', 'からあげ弁当'),
        quickPostbackItem('生姜焼き弁当', 'action=menu&item=shogayaki', '生姜焼き弁当'),
        quickPostbackItem('ハンバーグ弁当', 'action=menu&item=hamburger', 'ハンバーグ弁当'),
        quickPostbackItem('日替わり弁当', 'action=menu&item=daily', '日替わり弁当')
      ]
    }
  };
}

function buildQtyMessage(menuName) {
  return {
    type: 'text',
    text: `${menuName} の個数を選んでください。`,
    quickReply: {
      items: [
        quickPostbackItem('1個', 'action=qty&value=1', '1個'),
        quickPostbackItem('2個', 'action=qty&value=2', '2個'),
        quickPostbackItem('3個', 'action=qty&value=3', '3個'),
        quickPostbackItem('4個', 'action=qty&value=4', '4個'),
        quickPostbackItem('5個', 'action=qty&value=5', '5個')
      ]
    }
  };
}

function buildConfirmMessage(session) {
  return {
    type: 'text',
    text:
      '以下の内容で予約します。\n\n' +
      `【受取日】${session.date}\n` +
      `【受取時間】${session.time}\n` +
      `【商品】${session.menuName}\n` +
      `【個数】${session.qty}個\n` +
      `【合計】¥${session.total}\n` +
      `【お名前】${session.name}\n` +
      `【電話番号】${session.phone}`,
    quickReply: {
      items: [
        quickPostbackItem('予約確定', 'action=confirm', '予約確定'),
        quickPostbackItem('最初からやり直す', 'action=restart', '最初からやり直す')
      ]
    }
  };
}

function quickPostbackItem(label, data, displayText) {
  return {
    type: 'action',
    action: {
      type: 'postback',
      label,
      data,
      displayText
    }
  };
}

function textMessage(text) {
  return {
    type: 'text',
    text
  };
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

async function saveReservationToSheet(reservation) {
  if (!RESERVATION_SAVE_URL) {
    return { ok: false, error: 'RESERVATION_SAVE_URL が未設定です' };
  }

  try {
    const url = new URL(RESERVATION_SAVE_URL);
    url.searchParams.set('reservationNo', reservation.reservationNo || '');
    url.searchParams.set('date', reservation.date || '');
    url.searchParams.set('time', reservation.time || '');
    url.searchParams.set('menuName', reservation.menuName || '');
    url.searchParams.set('qty', String(reservation.qty || ''));
    url.searchParams.set('price', String(reservation.price || ''));
    url.searchParams.set('total', String(reservation.total || ''));
    url.searchParams.set('name', reservation.name || '');
    url.searchParams.set('phone', reservation.phone || '');
    url.searchParams.set('userId', reservation.userId || '');
    url.searchParams.set('status', reservation.status || '');
    url.searchParams.set('createdAt', reservation.createdAt || '');

    const response = await fetch(url.toString(), {
      method: 'GET',
      redirect: 'follow'
    });

    const text = await response.text();
    console.log('sheet response:', text);

    if (!response.ok) {
      return { ok: false, error: `HTTP ${response.status}: ${text}` };
    }

    let json;
    try {
      json = JSON.parse(text);
    } catch {
      return { ok: false, error: `JSON parse error: ${text}` };
    }

    if (!json.ok) {
      return { ok: false, error: json.error || 'Apps Script returned ok:false' };
    }

    return { ok: true };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

function verifySignature(rawBody, signature, secret) {
  if (!secret || !signature) return false;

  const hash = crypto
    .createHmac('SHA256', secret)
    .update(rawBody)
    .digest('base64');

  try {
    return crypto.timingSafeEqual(Buffer.from(hash), Buffer.from(signature));
  } catch {
    return false;
  }
}

function parsePostbackData(data) {
  const obj = {};
  const pairs = data.split('&');

  for (const pair of pairs) {
    const [key, value = ''] = pair.split('=');
    if (key) obj[key] = decodeURIComponent(value);
  }

  return obj;
}

function normalizePhone(text) {
  return String(text).replace(/[^\d]/g, '');
}

function isValidPhone(phone) {
  return /^\d{10,11}$/.test(phone);
}

function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

function formatDate(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function isTomorrowOrLater(dateStr) {
  const today = new Date();
  const tomorrow = new Date(today.getFullYear(), today.getMonth(), today.getDate() + 1);
  const selected = new Date(`${dateStr}T00:00:00`);
  return selected.getTime() >= tomorrow.getTime();
}

function isReservationComplete(session) {
  return !!(
    session.date &&
    session.time &&
    session.menuKey &&
    session.menuName &&
    session.price &&
    session.qty &&
    session.total &&
    session.name &&
    session.phone
  );
}

function createReservationNo() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  const hh = String(now.getHours()).padStart(2, '0');
  const mm = String(now.getMinutes()).padStart(2, '0');
  const ss = String(now.getSeconds()).padStart(2, '0');
  return `LB-${y}${m}${d}-${hh}${mm}${ss}`;
}

function clearSession(userId) {
  sessions.set(userId, {});
}

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});
