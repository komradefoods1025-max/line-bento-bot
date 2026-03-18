const express = require('express');
const crypto = require('crypto');

const app = express();

const PORT = process.env.PORT || 10000;
const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET || '';
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const RESERVATION_SAVE_URL = process.env.RESERVATION_SAVE_URL || '';
const STORE_NOTIFY_LINE_ID = process.env.STORE_NOTIFY_LINE_ID || '';

const STORE_NAME = 'かむらど';
const STORE_CODE = 'KMR';
const TIME_ZONE = 'Asia/Tokyo';
const BOOKABLE_DATE_COUNT = 10;

const DEFAULT_DAILY_MENU = {
  name: '日替わり弁当',
  price: 800,
  description: ''
};

const MENUS = {
  karaage: { name: 'からあげ弁当', price: 850 },
  shogayaki: { name: '生姜焼き弁当', price: 900 },
  hamburger: { name: 'ハンバーグ弁当', price: 950 }
};

const PICKUP_TIMES = [
  '11:30', '11:45', '12:00', '12:15', '12:30',
  '12:45', '13:00', '13:15', '13:30'
];

const sessions = new Map();

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
  const replyToken = event.replyToken;
  if (!replyToken) return;

  const sourceId =
    event.source?.userId ||
    event.source?.groupId ||
    event.source?.roomId ||
    '';

  const userId = event.source?.userId || null;
  const session = userId ? getSession(userId) : null;

  if (event.type === 'follow' && userId) {
    clearSession(userId);
    await replyMessage(replyToken, [startGuideMessage()]);
    return;
  }

  if (event.type === 'message' && event.message?.type === 'text') {
    const text = (event.message.text || '').trim();

    if (text === '通知先ID') {
      await replyMessage(replyToken, [
        textMessage(
          `現在の通知先IDはこちらです。\n\n${sourceId}\n\n` +
          `このIDを Render の STORE_NOTIFY_LINE_ID に入れてください。`
        )
      ]);
      return;
    }

    if (!userId) return;

    if ([
      '予約',
      '弁当予約',
      'ランチ弁当予約',
      '予約したい',
      '予約を始める',
      'テイクアウト予約'
    ].includes(text)) {
      await beginReservationFlow(replyToken, userId);
      return;
    }

    if (['最初から', 'やり直し', 'リセット'].includes(text)) {
      await beginReservationFlow(replyToken, userId);
      return;
    }

    if (['注文確認', '注文内容確認', '確認'].includes(text)) {
      if (!session.items || session.items.length === 0) {
        await replyMessage(replyToken, [
          textMessage('まだ商品が入っていません。'),
          buildMenuMessage(session)
        ]);
        return;
      }

      session.step = 'waiting_name';
      await replyMessage(replyToken, [
        buildCartSummaryMessage(session),
        textMessage('ご予約名を入力してください。')
      ]);
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
    return;
  }

  if (event.type === 'postback' && userId) {
    const data = parsePostbackData(event.postback?.data || '');

    if (data.action === 'reserve_start' || data.action === 'restart') {
      await beginReservationFlow(replyToken, userId);
      return;
    }

    if (data.action === 'pick_date') {
      const selectedDate = data.date || '';

      if (!selectedDate) {
        await beginReservationFlow(replyToken, userId);
        return;
      }

      if (!session.availableDates.includes(selectedDate)) {
        await replyMessage(replyToken, [
          textMessage('その日は受付対象外です。営業日から選び直してください。'),
          buildDateOptionsMessage(session.availableDateOptions || [])
        ]);
        return;
      }

      session.date = selectedDate;
      session.dailyMenu = await fetchDailyMenuConfig(selectedDate);
      session.step = 'waiting_time';

      const messages = [
        textMessage(`受取日：${formatDateWithWeekday(selectedDate)}`)
      ];

      if (session.dailyMenu && session.dailyMenu.name) {
        messages.push(
          textMessage(
            `★この日の日替わり★\n` +
            `${session.dailyMenu.name}　¥${Number(session.dailyMenu.price).toLocaleString('ja-JP')}` +
            (session.dailyMenu.description ? `\n${session.dailyMenu.description}` : '')
          )
        );
      }

      messages.push(buildTimeMessage());

      await replyMessage(replyToken, messages);
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
        buildMenuMessage(session)
      ]);
      return;
    }

    if (data.action === 'menu') {
      const item = data.item || '';
      const menu = resolveMenuByKey(session, item);

      if (!menu) {
        await replyMessage(replyToken, [
          textMessage('メニューが見つかりませんでした。もう一度選んでください。'),
          buildMenuMessage(session)
        ]);
        return;
      }

      session.currentSelection = {
        menuKey: item,
        menuName: menu.name,
        price: menu.price
      };
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
          buildQtyMessage(session.currentSelection?.menuName || 'お弁当')
        ]);
        return;
      }

      if (!session.currentSelection) {
        await replyMessage(replyToken, [
          textMessage('先に商品を選んでください。'),
          buildMenuMessage(session)
        ]);
        return;
      }

      addItemToCart(session, {
        menuKey: session.currentSelection.menuKey,
        menuName: session.currentSelection.menuName,
        price: session.currentSelection.price,
        qty
      });

      const addedName = session.currentSelection.menuName;
      session.currentSelection = null;
      session.step = 'menu_or_review';

      await replyMessage(replyToken, [
        textMessage(`${addedName} を ${qty}個 追加しました。`),
        buildCartSummaryMessage(session),
        buildCartActionMessage()
      ]);
      return;
    }

    if (data.action === 'add_more') {
      if (!session.items || session.items.length === 0) {
        await replyMessage(replyToken, [
          textMessage('まだ商品が入っていません。'),
          buildMenuMessage(session)
        ]);
        return;
      }

      session.step = 'waiting_menu';
      await replyMessage(replyToken, [buildMenuMessage(session)]);
      return;
    }

    if (data.action === 'review_order') {
      if (!session.items || session.items.length === 0) {
        await replyMessage(replyToken, [
          textMessage('まだ商品が入っていません。'),
          buildMenuMessage(session)
        ]);
        return;
      }

      session.step = 'waiting_name';
      await replyMessage(replyToken, [
        buildCartSummaryMessage(session),
        textMessage('ご予約名を入力してください。')
      ]);
      return;
    }

    if (data.action === 'confirm') {
      if (!isReservationComplete(session)) {
        await beginReservationFlow(replyToken, userId);
        return;
      }

      const reservationNo = createReservationNo();
      const items = session.items.map((item) => ({
        menuKey: item.menuKey,
        menuName: item.menuName,
        price: item.price,
        qty: item.qty,
        total: item.total
      }));

      const reservation = {
        reservationNo,
        userId,
        date: session.date,
        time: session.time,
        items,
        itemCount: items.length,
        totalQty: getCartTotalQty(items),
        total: getCartTotalAmount(items),
        name: session.name,
        phone: session.phone,
        status: '受付済み',
        createdAt: getJstDateTimeLabel()
      };

      const saveResult = await saveReservationToSheet(reservation);

      if (!saveResult.ok) {
        console.error('sheet save error:', saveResult.error);
        await replyMessage(replyToken, [
          textMessage(`予約内容の保存でエラーが起きました。\n${saveResult.error}`)
        ]);
        return;
      }

      notifyStoreByLine(reservation).catch((err) => {
        console.error('store line notify error:', err);
      });

      clearSession(userId);

      await replyMessage(replyToken, [
        buildReservationCompleteMessage(reservation)
      ]);
      return;
    }
  }
}

async function beginReservationFlow(replyToken, userId) {
  clearSession(userId);
  const session = getSession(userId);

  const bookingConfig = await fetchBookingConfig();

  if (!bookingConfig.ok || !bookingConfig.dates || bookingConfig.dates.length === 0) {
    await replyMessage(replyToken, [
      textMessage('現在ご案内できる営業日がありません。時間をおいてお試しください。')
    ]);
    return;
  }

  session.availableDateOptions = bookingConfig.dates;
  session.availableDates = bookingConfig.dates.map((item) => item.date);

  await replyMessage(replyToken, [
    textMessage(
      `${STORE_NAME}のランチ弁当予約です。\n` +
      `ご予約は前日${pad2(bookingConfig.deadlineHour || 22)}:00までです。\n` +
      `営業日のみ表示しています。`
    ),
    buildDateOptionsMessage(bookingConfig.dates)
  ]);
}

function resolveMenuByKey(session, key) {
  if (key === 'daily') {
    return session.dailyMenu || DEFAULT_DAILY_MENU;
  }
  return MENUS[key] || null;
}

function startGuideMessage() {
  return {
    type: 'text',
    text:
      `${STORE_NAME}のランチ弁当予約です。\n` +
      '下のボタンから始めてください。',
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

function buildDateOptionsMessage(dateOptions) {
  return {
    type: 'text',
    text: '受取日を選んでください。',
    quickReply: {
      items: dateOptions.map((item) =>
        quickPostbackItem(item.label, `action=pick_date&date=${encodeURIComponent(item.date)}`, item.label)
      )
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

function buildMenuMessage(session) {
  const dailyMenu = session.dailyMenu || DEFAULT_DAILY_MENU;

  const menuText =
    'ご希望のお弁当を選んでください。\n\n' +
    `・からあげ弁当 ¥${MENUS.karaage.price}\n` +
    `・生姜焼き弁当 ¥${MENUS.shogayaki.price}\n` +
    `・ハンバーグ弁当 ¥${MENUS.hamburger.price}\n` +
    `・${dailyMenu.name} ¥${dailyMenu.price}` +
    (dailyMenu.description ? `\n  ${dailyMenu.description}` : '');

  let cartText = '';
  if (session.items && session.items.length > 0) {
    cartText =
      '\n\n現在のご注文\n' +
      formatOrderLines(session.items) +
      `\n合計個数：${getCartTotalQty(session.items)}個` +
      `\n注文合計：¥${Number(getCartTotalAmount(session.items)).toLocaleString('ja-JP')}`;
  }

  const items = [
    quickPostbackItem('からあげ弁当', 'action=menu&item=karaage', 'からあげ弁当'),
    quickPostbackItem('生姜焼き弁当', 'action=menu&item=shogayaki', '生姜焼き弁当'),
    quickPostbackItem('ハンバーグ弁当', 'action=menu&item=hamburger', 'ハンバーグ弁当'),
    quickPostbackItem(truncateLabel(dailyMenu.name), 'action=menu&item=daily', dailyMenu.name)
  ];

  if (session.items && session.items.length > 0) {
    items.push(quickPostbackItem('注文内容を確認', 'action=review_order', '注文内容を確認'));
  }

  return {
    type: 'text',
    text: menuText + cartText,
    quickReply: { items }
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

function buildCartSummaryMessage(session) {
  const items = session.items || [];
  return textMessage(
    '現在のご注文内容です。\n\n' +
    formatOrderLines(items) +
    `\n合計個数：${getCartTotalQty(items)}個` +
    `\n注文合計：¥${Number(getCartTotalAmount(items)).toLocaleString('ja-JP')}`
  );
}

function buildCartActionMessage() {
  return {
    type: 'text',
    text: '続けて商品を追加するか、注文内容を確認してください。',
    quickReply: {
      items: [
        quickPostbackItem('他の商品を追加', 'action=add_more', '他の商品を追加'),
        quickPostbackItem('注文内容を確認', 'action=review_order', '注文内容を確認'),
        quickPostbackItem('最初からやり直す', 'action=restart', '最初からやり直す')
      ]
    }
  };
}

function buildConfirmMessage(session) {
  const items = session.items || [];
  return {
    type: 'text',
    text:
      '以下の内容で予約します。\n\n' +
      `【受取日】${formatDateWithWeekday(session.date)}\n` +
      `【受取時間】${session.time}\n` +
      `【ご注文内容】\n${formatOrderLines(items)}\n` +
      `【合計個数】${getCartTotalQty(items)}個\n` +
      `【注文合計】¥${Number(getCartTotalAmount(items)).toLocaleString('ja-JP')}\n` +
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

function buildReservationCompleteMessage(reservation) {
  return {
    type: 'text',
    text:
      `【${STORE_NAME} ご予約受付完了】\n\n` +
      `受付番号：${reservation.reservationNo}\n` +
      `受取日：${formatDateWithWeekday(reservation.date)}\n` +
      `受取時間：${reservation.time}\n` +
      `ご注文内容：\n${formatOrderLines(reservation.items)}\n` +
      `合計個数：${reservation.totalQty}個\n` +
      `注文合計：¥${Number(reservation.total).toLocaleString('ja-JP')}\n` +
      `お名前：${reservation.name}\n` +
      `電話番号：${reservation.phone}\n\n` +
      `※お支払いは店頭にてお願いいたします。\n` +
      `※受付番号をご来店時にお伝えください。`
  };
}

function truncateLabel(text) {
  return text.length > 20 ? text.slice(0, 20) : text;
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
  return { type: 'text', text };
}

function getSession(userId) {
  if (!sessions.has(userId)) {
    sessions.set(userId, {
      items: [],
      currentSelection: null,
      dailyMenu: DEFAULT_DAILY_MENU,
      availableDates: [],
      availableDateOptions: []
    });
  }
  return sessions.get(userId);
}

function clearSession(userId) {
  sessions.set(userId, {
    items: [],
    currentSelection: null,
    dailyMenu: DEFAULT_DAILY_MENU,
    availableDates: [],
    availableDateOptions: []
  });
}

function addItemToCart(session, newItem) {
  if (!session.items) session.items = [];

  const existing = session.items.find((item) => item.menuKey === newItem.menuKey);

  if (existing) {
    existing.qty += newItem.qty;
    existing.total = existing.qty * existing.price;
  } else {
    session.items.push({
      menuKey: newItem.menuKey,
      menuName: newItem.menuName,
      price: newItem.price,
      qty: newItem.qty,
      total: newItem.price * newItem.qty
    });
  }
}

function formatOrderLines(items) {
  if (!items || items.length === 0) return '・商品が入っていません';

  return items
    .map((item) => `・${item.menuName} ×${item.qty}個　¥${Number(item.total).toLocaleString('ja-JP')}`)
    .join('\n');
}

function getCartTotalQty(items) {
  return (items || []).reduce((sum, item) => sum + Number(item.qty || 0), 0);
}

function getCartTotalAmount(items) {
  return (items || []).reduce((sum, item) => sum + Number(item.total || 0), 0);
}

async function fetchBookingConfig() {
  if (!RESERVATION_SAVE_URL) {
    return { ok: false, error: 'RESERVATION_SAVE_URL が未設定です' };
  }

  try {
    const url = new URL(RESERVATION_SAVE_URL);
    url.searchParams.set('action', 'getBookingConfig');
    url.searchParams.set('count', String(BOOKABLE_DATE_COUNT));

    const response = await fetch(url.toString(), {
      method: 'GET',
      redirect: 'follow'
    });

    const text = await response.text();

    if (!response.ok) {
      return { ok: false, error: `HTTP ${response.status}: ${text}` };
    }

    const json = JSON.parse(text);
    if (!json.ok) {
      return { ok: false, error: json.error || 'booking config error' };
    }

    return json;
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function fetchDailyMenuConfig(dateStr) {
  if (!RESERVATION_SAVE_URL) {
    return DEFAULT_DAILY_MENU;
  }

  try {
    const url = new URL(RESERVATION_SAVE_URL);
    url.searchParams.set('action', 'getDailyMenu');
    url.searchParams.set('date', dateStr);

    const response = await fetch(url.toString(), {
      method: 'GET',
      redirect: 'follow'
    });

    const text = await response.text();

    if (!response.ok) {
      return DEFAULT_DAILY_MENU;
    }

    const json = JSON.parse(text);

    if (!json.ok || !json.found) {
      return DEFAULT_DAILY_MENU;
    }

    return {
      name: json.menuName || DEFAULT_DAILY_MENU.name,
      price: Number(json.price || DEFAULT_DAILY_MENU.price),
      description: json.description || ''
    };
  } catch (_err) {
    return DEFAULT_DAILY_MENU;
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
    url.searchParams.set('name', reservation.name || '');
    url.searchParams.set('phone', reservation.phone || '');
    url.searchParams.set('userId', reservation.userId || '');
    url.searchParams.set('status', reservation.status || '');
    url.searchParams.set('createdAt', reservation.createdAt || '');
    url.searchParams.set('itemCount', String(reservation.itemCount || ''));
    url.searchParams.set('totalQty', String(reservation.totalQty || ''));
    url.searchParams.set('total', String(reservation.total || ''));
    url.searchParams.set('itemsJson', JSON.stringify(reservation.items || []));

    const response = await fetch(url.toString(), {
      method: 'GET',
      redirect: 'follow'
    });

    const text = await response.text();

    if (!response.ok) {
      return { ok: false, error: `HTTP ${response.status}: ${text}` };
    }

    const json = JSON.parse(text);
    if (!json.ok) {
      return { ok: false, error: json.error || 'Apps Script returned ok:false' };
    }

    return { ok: true };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function notifyStoreByLine(reservation) {
  if (!STORE_NOTIFY_LINE_ID) return;

  const message = {
    type: 'text',
    text:
      `【店舗通知：新規ランチ予約】\n\n` +
      `受付番号：${reservation.reservationNo}\n` +
      `受取日：${formatDateWithWeekday(reservation.date)}\n` +
      `受取時間：${reservation.time}\n` +
      `ご注文内容：\n${formatOrderLines(reservation.items)}\n` +
      `合計個数：${reservation.totalQty}個\n` +
      `注文合計：¥${Number(reservation.total).toLocaleString('ja-JP')}\n` +
      `お名前：${reservation.name}\n` +
      `電話番号：${reservation.phone}`
  };

  const response = await fetch('https://api.line.me/v2/bot/message/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({
      to: STORE_NOTIFY_LINE_ID,
      messages: [message]
    })
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Push API error: ${response.status} ${text}`);
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

function isReservationComplete(session) {
  return !!(
    session.date &&
    session.time &&
    session.items &&
    session.items.length > 0 &&
    session.name &&
    session.phone
  );
}

function createReservationNo() {
  const parts = getJstParts();
  return `${STORE_CODE}-${pad2(parts.month)}${pad2(parts.day)}-${pad2(parts.hour)}${pad2(parts.minute)}${pad2(parts.second)}`;
}

function getJstDateTimeLabel() {
  const parts = getJstParts();
  return `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)} ${pad2(parts.hour)}:${pad2(parts.minute)}:${pad2(parts.second)}`;
}

function getJstParts(date = new Date()) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });

  const map = {};
  for (const part of formatter.formatToParts(date)) {
    if (part.type !== 'literal') {
      map[part.type] = part.value;
    }
  }

  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour: Number(map.hour),
    minute: Number(map.minute),
    second: Number(map.second)
  };
}

function pad2(value) {
  return String(value).padStart(2, '0');
}

function formatDateWithWeekday(dateStr) {
  return `${dateStr}（${getWeekdayJa(dateStr)}）`;
}

function getWeekdayJa(dateStr) {
  const date = utcDateFromYmd(dateStr);
  return new Intl.DateTimeFormat('ja-JP', {
    weekday: 'short',
    timeZone: 'UTC'
  }).format(date);
}

function utcDateFromYmd(ymd) {
  const [year, month, day] = ymd.split('-').map(Number);
  return new Date(Date.UTC(year, month - 1, day));
}

function addDaysToYmd(ymd, days) {
  const date = utcDateFromYmd(ymd);
  date.setUTCDate(date.getUTCDate() + days);
  return `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;
}

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});
