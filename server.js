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

const PENDING_REMINDER_MINUTES = Number(process.env.PENDING_REMINDER_MINUTES || 5);
const REMINDER_CRON_TOKEN = process.env.REMINDER_CRON_TOKEN || '';

const DEFAULT_DAILY_MENU = {
  name: '日替わり弁当',
  price: 600,
  description: 'その日のお楽しみメニューです',
  imageUrl: 'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/e38380e382a6e383b3e383ade383bce38389.jpeg'
};

const MENUS = {
  karaage: {
    name: 'からあげ弁当',
    price: 700,
    description: 'ジューシーな唐揚げが人気の定番弁当',
    imageUrl: 'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/photo_2026-03-22_13-12-15.jpg'
  },
  shogayaki: {
    name: '生姜焼き弁当',
    price: 700,
    description: '香ばしく焼き上げたごはんが進む一品',
    imageUrl: 'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/photo_2026-03-22_14-13-55.jpg'
  },
  chicken_nanban: {
    name: 'チキン南蛮弁当',
    price: 900,
    description: 'オリジナルタルタルが美味な至極の一品',
    imageUrl: 'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/photo_2026-03-22_14-35-09.jpg'
  }
};

const EXTRA_KARAAGE_KEY = 'extra_karaage';

const EXTRA_MENUS = {
  [EXTRA_KARAAGE_KEY]: {
    name: '追加唐揚げ',
    price: 80,
    description: 'お弁当に追加できる唐揚げです（1個80円）',
    imageUrl: 'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/photo_2026-03-22_14-58-55.jpg'
  }
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

app.get('/tasks/remind-pending', async (req, res) => {
  try {
    const token = req.query.token || req.get('x-cron-token') || '';
    if (REMINDER_CRON_TOKEN && token !== REMINDER_CRON_TOKEN) {
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }

    const result = await runPendingReminderJob();
    return res.status(200).json(result);
  } catch (err) {
    console.error('remind-pending error:', err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
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
  const session = userId ? await loadSession(userId) : null;

  if (event.type === 'follow' && userId) {
    clearSession(userId);
    await clearPendingSession(userId);
    await replyMessage(replyToken, [startGuideMessage()]);
    return;
  }

  if (event.type === 'message' && event.message?.type === 'text') {
    const rawText = event.message.text || '';
    const text = rawText.trim();

    console.log('incoming text:', rawText);

    if (isNotifyIdText(text)) {
      await replyMessage(replyToken, [
        textMessage(
          `現在の通知先IDはこちらです。\n\n${sourceId}\n\nこのIDを Render の STORE_NOTIFY_LINE_ID に入れてください。`
        )
      ]);
      return;
    }

    if (!userId) {
      await replyMessage(replyToken, [
        textMessage('予約は bot との1対1トークでご利用ください。')
      ]);
      return;
    }

    if (isReservationStartText(text)) {
      await beginReservationFlow(replyToken, userId);
      return;
    }

    if (isResetText(text)) {
      await beginReservationFlow(replyToken, userId);
      return;
    }

    if (isResumeText(text)) {
      if (hasActiveSession(session)) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, buildResumeMessages(session));
        return;
      }

      await beginReservationFlow(replyToken, userId);
      return;
    }

    if (isReviewText(text)) {
      if (!session.items.length) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('まだ商品が入っていません。'),
          ...buildMenuStepMessages(session)
        ]);
        return;
      }

      session.step = 'waiting_name';
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        buildCartSummaryMessage(session),
        textMessage('ご予約名を入力してください。')
      ]);
      return;
    }

    if (session.step === 'waiting_name') {
      session.name = text;
      session.step = 'waiting_phone';
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`ご予約名：${text}`),
        textMessage('電話番号を入力してください。\n例：09012345678')
      ]);
      return;
    }

    if (session.step === 'waiting_phone') {
      const phone = normalizePhone(text);

      if (!isValidPhone(phone)) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('電話番号の形式が正しくありません。\n数字のみで入力してください。\n例：09012345678')
        ]);
        return;
      }

      session.phone = phone;
      session.step = 'confirm';
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`電話番号：${phone}`),
        buildConfirmMessage(session)
      ]);
      return;
    }

    if (hasActiveSession(session)) {
      await savePendingSession(userId, session);
      await replyMessage(replyToken, buildResumeMessages(session));
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

      if (!selectedDate || !session.availableDates.includes(selectedDate)) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('その日は受付対象外です。営業日から選び直してください。'),
          buildDateOptionsMessage(session.availableDateOptions)
        ]);
        return;
      }

      session.date = selectedDate;
      session.dailyMenu = await fetchDailyMenuConfig(selectedDate);
      session.step = 'waiting_time';
      await savePendingSession(userId, session);

      const messages = [textMessage(`受取日：${formatDateWithWeekday(selectedDate)}`)];

      if (session.dailyMenu?.name) {
        messages.push(
          textMessage(
            `★この日の日替わり★\n${session.dailyMenu.name}　¥${Number(session.dailyMenu.price).toLocaleString('ja-JP')}` +
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
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('受取時間をもう一度選んでください。'),
          buildTimeMessage()
        ]);
        return;
      }

      session.time = selectedTime;
      session.step = 'waiting_menu';
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`受取時間：${selectedTime}`),
        ...buildMenuStepMessages(session)
      ]);
      return;
    }

    if (data.action === 'menu') {
      const menu = resolveMenuByKey(session, data.item || '');

      if (!menu) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('メニューが見つかりませんでした。'),
          ...buildMenuStepMessages(session)
        ]);
        return;
      }

      session.currentSelection = {
        menuKey: data.item,
        menuName: menu.name,
        price: menu.price
      };
      session.step = 'waiting_qty';
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`ご注文商品：${menu.name}`),
        buildQtyMessage(menu.name)
      ]);
      return;
    }

    if (data.action === 'qty') {
      const qty = Number(data.value || 0);

      if (!qty || !session.currentSelection) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('個数をもう一度選んでください。'),
          buildQtyMessage(session.currentSelection?.menuName || '商品')
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
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`${addedName} を ${qty}個 追加しました。`),
        buildCartSummaryMessage(session),
        buildCartActionMessage()
      ]);
      return;
    }

    if (data.action === 'add_more') {
      session.step = 'waiting_menu';
      await savePendingSession(userId, session);
      await replyMessage(replyToken, buildMenuStepMessages(session));
      return;
    }

    if (data.action === 'review_order') {
      if (!session.items.length) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('まだ商品が入っていません。'),
          ...buildMenuStepMessages(session)
        ]);
        return;
      }

      session.step = 'waiting_name';
      await savePendingSession(userId, session);

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

      const reservation = {
        reservationNo: createReservationNo(),
        userId,
        date: session.date,
        time: session.time,
        items: session.items.map((item) => ({ ...item })),
        itemCount: session.items.length,
        totalQty: getCartTotalQty(session.items),
        total: getCartTotalAmount(session.items),
        name: session.name,
        phone: session.phone,
        status: '受付済み',
        createdAt: getJstDateTimeLabel()
      };

      const saveResult = await saveReservationToSheet(reservation);

      if (!saveResult.ok) {
        await replyMessage(replyToken, [
          textMessage(`予約内容の保存でエラーが起きました。\n${saveResult.error}`)
        ]);
        return;
      }

      notifyStoreByLine(reservation).catch((err) =>
        console.error('store line notify error:', err)
      );

      clearSession(userId);
      await clearPendingSession(userId);

      await replyMessage(replyToken, [
        buildReservationCompleteMessage(reservation)
      ]);
      return;
    }
  }
}

async function beginReservationFlow(replyToken, userId) {
  clearSession(userId);
  await clearPendingSession(userId);

  const session = getSession(userId);
  const bookingConfig = await fetchBookingConfig();

  if (!bookingConfig.ok || !bookingConfig.dates?.length) {
    await replyMessage(replyToken, [
      textMessage('現在ご案内できる営業日がありません。時間をおいてお試しください。')
    ]);
    return;
  }

  session.availableDateOptions = bookingConfig.dates;
  session.availableDates = bookingConfig.dates.map((item) => item.date);
  session.step = 'waiting_date';

  await savePendingSession(userId, session);

  await replyMessage(replyToken, [
    textMessage(`${STORE_NAME}のランチ弁当予約です！\n営業日のみ表示しています🗓️`),
    buildDateOptionsMessage(bookingConfig.dates)
  ]);
}

function buildDateOptionsMessage(dateOptions) {
  return {
    type: 'text',
    text: '受取日を選んでください📆',
    quickReply: {
      items: (dateOptions || []).map((item) =>
        quickPostbackItem(
          item.label,
          `action=pick_date&date=${encodeURIComponent(item.date)}`,
          item.label
        )
      )
    }
  };
}

function buildTimeMessage() {
  return {
    type: 'text',
    text: '受取時間を選んでください⏰',
    quickReply: {
      items: PICKUP_TIMES.map((time) =>
        quickPostbackItem(
          time,
          `action=time&value=${encodeURIComponent(time)}`,
          time
        )
      )
    }
  };
}

function buildMenuStepMessages(session) {
  const messages = [];
  let intro = 'ご希望の商品をお選びください🍱';

  if (session.dailyMenu?.name) {
    intro +=
      `\n\n★本日の日替わり★\n${session.dailyMenu.name}　¥${Number(session.dailyMenu.price).toLocaleString('ja-JP')}` +
      (session.dailyMenu.description ? `\n${session.dailyMenu.description}` : '');
  }

  if (session.items.length) {
    intro +=
      `\n\n現在のご注文\n${formatOrderLines(session.items)}` +
      `\n合計個数：${getCartTotalQty(session.items)}個` +
      `\n注文合計：¥${Number(getCartTotalAmount(session.items)).toLocaleString('ja-JP')}`;
  }

  messages.push(textMessage(intro));
  messages.push(buildMenuFlexMessage(session));
  return messages;
}

function buildMenuFlexMessage(session) {
  const dailyMenu = session.dailyMenu || DEFAULT_DAILY_MENU;

  return {
    type: 'flex',
    altText: 'お弁当メニュー',
    contents: {
      type: 'carousel',
      contents: [
        buildMenuBubble('karaage', MENUS.karaage),
        buildMenuBubble('shogayaki', MENUS.shogayaki),
        buildMenuBubble('chicken_nanban', MENUS.chicken_nanban),
        buildMenuBubble('daily', dailyMenu),
        buildMenuBubble(EXTRA_KARAAGE_KEY, EXTRA_MENUS[EXTRA_KARAAGE_KEY])
      ]
    }
  };
}

function buildMenuBubble(itemKey, menu) {
  const buttonLabel = itemKey === EXTRA_KARAAGE_KEY ? '追加する' : 'この商品を選ぶ';
  const displayText = itemKey === EXTRA_KARAAGE_KEY ? `${menu.name}を追加する` : `${menu.name}を選ぶ`;

  return {
    type: 'bubble',
    size: 'mega',
    hero: {
      type: 'image',
      url: menu.imageUrl,
      size: 'full',
      aspectRatio: '20:13',
      aspectMode: 'cover'
    },
    body: {
      type: 'box',
      layout: 'vertical',
      spacing: 'sm',
      contents: [
        {
          type: 'text',
          text: menu.name,
          weight: 'bold',
          size: 'lg',
          wrap: true
        },
        {
          type: 'text',
          text: `¥${Number(menu.price).toLocaleString('ja-JP')}`,
          weight: 'bold',
          size: 'md',
          color: '#16A34A'
        },
        {
          type: 'text',
          text: menu.description || '',
          size: 'sm',
          color: '#666666',
          wrap: true
        }
      ]
    },
    footer: {
      type: 'box',
      layout: 'vertical',
      contents: [
        {
          type: 'button',
          style: 'primary',
          action: {
            type: 'postback',
            label: buttonLabel,
            data: `action=menu&item=${itemKey}`,
            displayText
          }
        }
      ]
    }
  };
}

function buildQtyMessage(menuName) {
  return {
    type: 'text',
    text: `${menuName} の個数を選んでください🍚🍖`,
    quickReply: {
      items: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map((n) =>
        quickPostbackItem(`${n}個`, `action=qty&value=${n}`, `${n}個`)
      )
    }
  };
}

function buildCartSummaryMessage(session) {
  return textMessage(
    `現在のご注文内容です。\n\n${formatOrderLines(session.items)}` +
    `\n合計個数：${getCartTotalQty(session.items)}個` +
    `\n注文合計：¥${Number(getCartTotalAmount(session.items)).toLocaleString('ja-JP')}`
  );
}

function buildCartActionMessage() {
  return {
    type: 'text',
    text: '続けて商品を追加するか、注文内容を確認してください🔍',
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
  return {
    type: 'text',
    text:
      `以下の内容でよろしければ予約確定ボタンよりご注文を完了してください🙇\n\n` +
      `【受取日】${formatDateWithWeekday(session.date)}\n` +
      `【受取時間】${session.time}\n` +
      `【ご注文内容】\n${formatOrderLines(session.items)}\n` +
      `【合計個数】${getCartTotalQty(session.items)}個\n` +
      `【注文合計】¥${Number(getCartTotalAmount(session.items)).toLocaleString('ja-JP')}\n` +
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
  return textMessage(
    `ご注文ありがとうございます✨\n\n` +
      `受付番号：${reservation.reservationNo}\n` +
      `受取日：${formatDateWithWeekday(reservation.date)}\n` +
      `受取時間：${reservation.time}\n` +
      `ご注文内容：\n${formatOrderLines(reservation.items)}\n` +
      `合計個数：${reservation.totalQty}個\n` +
      `注文合計：¥${Number(reservation.total).toLocaleString('ja-JP')}\n` +
      `お名前：${reservation.name}\n` +
      `電話番号：${reservation.phone}`
  );
}

function startGuideMessage() {
  return {
    type: 'text',
    text: `${STORE_NAME}のランチ弁当予約です。\n「予約」と送っていただければ開始できます。`,
    quickReply: {
      items: [
        quickPostbackItem('予約を始める', 'action=reserve_start', '予約を始める')
      ]
    }
  };
}

function buildResumeMessages(session) {
  switch (session.step) {
    case 'waiting_date':
      return [
        textMessage('ご予約の続きから再開できます。'),
        buildDateOptionsMessage(session.availableDateOptions || [])
      ];

    case 'waiting_time': {
      const messages = [textMessage('ご予約の続きをご案内します。')];
      if (session.date) {
        messages.push(textMessage(`受取日：${formatDateWithWeekday(session.date)}`));
      }
      messages.push(buildTimeMessage());
      return messages;
    }

    case 'waiting_menu':
      return [
        textMessage('ご予約の続きをご案内します。'),
        ...buildMenuStepMessages(session)
      ];

    case 'waiting_qty':
      return [
        textMessage('ご予約の続きをご案内します。'),
        textMessage(`ご注文商品：${session.currentSelection?.menuName || '商品'}`),
        buildQtyMessage(session.currentSelection?.menuName || '商品')
      ];

    case 'menu_or_review':
      return [
        textMessage('ご予約の続きをご案内します。'),
        buildCartSummaryMessage(session),
        buildCartActionMessage()
      ];

    case 'waiting_name':
      return [
        buildCartSummaryMessage(session),
        textMessage('ご予約名を入力してください。')
      ];

    case 'waiting_phone':
      return [
        textMessage(`ご予約名：${session.name || ''}`),
        textMessage('電話番号を入力してください。\n例：09012345678')
      ];

    case 'confirm':
      return [buildConfirmMessage(session)];

    default:
      return [startGuideMessage()];
  }
}

function buildReminderMessages(session) {
  const head = textMessage('ご注文を続けますか？\n5分以上操作がなかったため、続きからご案内します。');

  switch (session.step) {
    case 'waiting_date':
      return [head, buildDateOptionsMessage(session.availableDateOptions || [])];

    case 'waiting_time':
      return [head, buildTimeMessage()];

    case 'waiting_menu':
      return [head, ...buildMenuStepMessages(session)];

    case 'waiting_qty':
      return [
        head,
        textMessage(`ご注文商品：${session.currentSelection?.menuName || '商品'}`),
        buildQtyMessage(session.currentSelection?.menuName || '商品')
      ];

    case 'menu_or_review':
      return [head, buildCartSummaryMessage(session), buildCartActionMessage()];

    case 'waiting_name':
      return [head, buildCartSummaryMessage(session), textMessage('ご予約名を入力してください。')];

    case 'waiting_phone':
      return [head, textMessage('電話番号を入力してください。\n例：09012345678')];

    case 'confirm':
      return [head, buildConfirmMessage(session)];

    default:
      return [head, startGuideMessage()];
  }
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

function resolveMenuByKey(session, key) {
  if (key === 'daily') {
    return session.dailyMenu || DEFAULT_DAILY_MENU;
  }

  if (EXTRA_MENUS[key]) {
    return EXTRA_MENUS[key];
  }

  return MENUS[key] || null;
}

async function loadSession(userId) {
  if (sessions.has(userId)) {
    return sessions.get(userId);
  }

  const pending = await getPendingSession(userId);

  if (pending.ok && pending.found) {
    const restored = restoreSessionFromPending(pending);
    sessions.set(userId, restored);
    return restored;
  }

  clearSession(userId);
  return sessions.get(userId);
}

function getSession(userId) {
  if (!sessions.has(userId)) {
    clearSession(userId);
  }
  return sessions.get(userId);
}

function clearSession(userId) {
  sessions.set(userId, {
    date: '',
    time: '',
    name: '',
    phone: '',
    items: [],
    currentSelection: null,
    dailyMenu: { ...DEFAULT_DAILY_MENU },
    availableDates: [],
    availableDateOptions: [],
    step: ''
  });
}

function restoreSessionFromPending(pending) {
  const items = safeJsonParse(pending.itemsJson, []);
  const currentSelection = safeJsonParse(pending.currentSelectionJson, null);
  const availableDates = safeJsonParse(pending.availableDatesJson, []);
  const availableDateOptions = safeJsonParse(pending.availableDateOptionsJson, []);
  const dailyMenu = safeJsonParse(pending.dailyMenuJson, { ...DEFAULT_DAILY_MENU });

  return {
    date: pending.date || '',
    time: pending.time || '',
    name: pending.name || '',
    phone: pending.phone || '',
    items: Array.isArray(items) ? items : [],
    currentSelection: currentSelection || null,
    dailyMenu: dailyMenu?.name ? dailyMenu : { ...DEFAULT_DAILY_MENU },
    availableDates: Array.isArray(availableDates) ? availableDates : [],
    availableDateOptions: Array.isArray(availableDateOptions) ? availableDateOptions : [],
    step: pending.step || ''
  };
}

function hasActiveSession(session) {
  if (!session) return false;

  return Boolean(
    session.step ||
    session.date ||
    session.time ||
    session.name ||
    session.phone ||
    (Array.isArray(session.items) && session.items.length > 0)
  );
}

function addItemToCart(session, newItem) {
  const existing = session.items.find((item) => item.menuKey === newItem.menuKey);

  if (existing) {
    existing.qty += newItem.qty;
    existing.total = existing.qty * existing.price;
    return;
  }

  session.items.push({
    menuKey: newItem.menuKey,
    menuName: newItem.menuName,
    price: newItem.price,
    qty: newItem.qty,
    total: newItem.price * newItem.qty
  });
}

function formatOrderLines(items) {
  if (!items || !items.length) return '・商品が入っていません';

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
  try {
    const url = buildReservationApiUrl({
      action: 'getBookingConfig',
      count: String(BOOKABLE_DATE_COUNT)
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text };
    return JSON.parse(text);
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function fetchDailyMenuConfig(dateStr) {
  try {
    const url = buildReservationApiUrl({
      action: 'getDailyMenu',
      date: dateStr
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return DEFAULT_DAILY_MENU;

    const json = JSON.parse(text);
    if (!json.ok || !json.found) return DEFAULT_DAILY_MENU;

    return {
      name: json.menuName || DEFAULT_DAILY_MENU.name,
      price: Number(json.price || DEFAULT_DAILY_MENU.price),
      description: json.description || '',
      imageUrl: DEFAULT_DAILY_MENU.imageUrl
    };
  } catch {
    return DEFAULT_DAILY_MENU;
  }
}

async function saveReservationToSheet(reservation) {
  try {
    const url = buildReservationApiUrl({
      reservationNo: reservation.reservationNo,
      date: reservation.date,
      time: reservation.time,
      name: reservation.name,
      phone: reservation.phone,
      userId: reservation.userId,
      status: reservation.status,
      createdAt: reservation.createdAt,
      itemCount: String(reservation.itemCount),
      totalQty: String(reservation.totalQty),
      total: String(reservation.total),
      itemsJson: JSON.stringify(reservation.items)
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text };

    const json = JSON.parse(text);
    return json.ok ? { ok: true } : { ok: false, error: json.error || 'save error' };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function savePendingSession(userId, session) {
  if (!userId || !RESERVATION_SAVE_URL) return { ok: false, error: 'missing config' };

  try {
    const nowMillis = Date.now();

    const url = buildReservationApiUrl({
      action: 'savePending',
      userId,
      step: session.step || '',
      lastActionAtMillis: String(nowMillis),
      lastActionAt: getJstDateTimeLabel(),
      date: session.date || '',
      time: session.time || '',
      itemsJson: JSON.stringify(session.items || []),
      currentSelectionJson: JSON.stringify(session.currentSelection || null),
      name: session.name || '',
      phone: session.phone || '',
      availableDatesJson: JSON.stringify(session.availableDates || []),
      availableDateOptionsJson: JSON.stringify(session.availableDateOptions || []),
      dailyMenuJson: JSON.stringify(session.dailyMenu || DEFAULT_DAILY_MENU)
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text };

    const json = JSON.parse(text);
    return json.ok ? { ok: true } : { ok: false, error: json.error || 'savePending error' };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function getPendingSession(userId) {
  if (!userId || !RESERVATION_SAVE_URL) return { ok: false, found: false };

  try {
    const url = buildReservationApiUrl({
      action: 'getPending',
      userId
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text, found: false };

    return JSON.parse(text);
  } catch (err) {
    return { ok: false, error: String(err), found: false };
  }
}

async function clearPendingSession(userId) {
  if (!userId || !RESERVATION_SAVE_URL) return { ok: true };

  try {
    const url = buildReservationApiUrl({
      action: 'clearPending',
      userId
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text };

    const json = JSON.parse(text);
    return json.ok ? { ok: true } : { ok: false, error: json.error || 'clearPending error' };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function fetchReminderTargets(minutes) {
  try {
    const url = buildReservationApiUrl({
      action: 'getReminderTargets',
      minutes: String(minutes)
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text, targets: [] };

    const json = JSON.parse(text);
    return json.ok ? json : { ok: false, error: json.error || 'getReminderTargets error', targets: [] };
  } catch (err) {
    return { ok: false, error: String(err), targets: [] };
  }
}

async function markReminderSent(userId) {
  try {
    const url = buildReservationApiUrl({
      action: 'markReminderSent',
      userId
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text };

    const json = JSON.parse(text);
    return json.ok ? { ok: true } : { ok: false, error: json.error || 'markReminderSent error' };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function runPendingReminderJob() {
  const result = {
    ok: true,
    checked: 0,
    pushed: 0,
    skipped: 0,
    failed: 0,
    details: []
  };

  const targetsResult = await fetchReminderTargets(PENDING_REMINDER_MINUTES);

  if (!targetsResult.ok) {
    return {
      ok: false,
      error: targetsResult.error || 'failed to fetch reminder targets'
    };
  }

  const targets = Array.isArray(targetsResult.targets) ? targetsResult.targets : [];
  result.checked = targets.length;

  for (const target of targets) {
    const userId = target.userId || '';

    if (!userId) {
      result.skipped += 1;
      result.details.push({ userId: '', status: 'skipped', reason: 'missing userId' });
      continue;
    }

    try {
      const pending = await getPendingSession(userId);

      if (!pending.ok || !pending.found) {
        result.skipped += 1;
        result.details.push({ userId, status: 'skipped', reason: 'pending not found' });
        continue;
      }

      const session = restoreSessionFromPending(pending);

      if (!hasActiveSession(session)) {
        result.skipped += 1;
        result.details.push({ userId, status: 'skipped', reason: 'inactive session' });
        continue;
      }

      const messages = buildReminderMessages(session);
      await pushMessage(userId, messages);
      await markReminderSent(userId);

      result.pushed += 1;
      result.details.push({ userId, status: 'pushed', step: session.step || '' });
    } catch (err) {
      result.failed += 1;
      result.details.push({
        userId,
        status: 'failed',
        error: err.message || String(err)
      });
    }
  }

  return result;
}

async function notifyStoreByLine(reservation) {
  if (!STORE_NOTIFY_LINE_ID) return;

  const response = await fetch('https://api.line.me/v2/bot/message/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({
      to: STORE_NOTIFY_LINE_ID,
      messages: [
        textMessage(
          `【店舗通知：新規ランチ予約】\n\n` +
          `受付番号：${reservation.reservationNo}\n` +
          `受取日：${formatDateWithWeekday(reservation.date)}\n` +
          `受取時間：${reservation.time}\n` +
          `ご注文内容：\n${formatOrderLines(reservation.items)}\n` +
          `合計個数：${reservation.totalQty}個\n` +
          `注文合計：¥${Number(reservation.total).toLocaleString('ja-JP')}\n` +
          `お名前：${reservation.name}\n` +
          `電話番号：${reservation.phone}`
        )
      ]
    })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text);
  }
}

async function replyMessage(replyToken, messages) {
  const response = await fetch('https://api.line.me/v2/bot/message/reply', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({ replyToken, messages })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Reply API error: ${response.status} ${text}`);
  }
}

async function pushMessage(to, messages) {
  const response = await fetch('https://api.line.me/v2/bot/message/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({ to, messages })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Push API error: ${response.status} ${text}`);
  }
}

function buildReservationApiUrl(params) {
  if (!RESERVATION_SAVE_URL) {
    throw new Error('RESERVATION_SAVE_URL is not set');
  }

  const url = new URL(RESERVATION_SAVE_URL);

  Object.entries(params || {}).forEach(([key, value]) => {
    url.searchParams.set(key, value == null ? '' : String(value));
  });

  return url.toString();
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
  const out = {};
  for (const pair of data.split('&')) {
    const [key, value = ''] = pair.split('=');
    if (key) out[key] = decodeURIComponent(value);
  }
  return out;
}

function safeJsonParse(text, fallback) {
  try {
    const parsed = JSON.parse(text);
    return parsed == null ? fallback : parsed;
  } catch {
    return fallback;
  }
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
    session.items.length &&
    session.name &&
    session.phone
  );
}

function normalizeIncomingText(text) {
  return String(text || '')
    .normalize('NFKC')
    .replace(/\s+/g, '')
    .replace(/[！!？?。．、,，]/g, '')
    .trim();
}

function isReservationStartText(text) {
  const t = normalizeIncomingText(text);

  if (!t) return false;
  if (t.includes('予約')) return true;

  return ['弁当', 'ランチ', 'テイクアウト']
    .some((keyword) => t.includes(keyword) && t.includes('したい'));
}

function isResetText(text) {
  const t = normalizeIncomingText(text);
  return ['最初から', 'やり直し', 'リセット'].includes(t);
}

function isReviewText(text) {
  const t = normalizeIncomingText(text);
  return ['注文確認', '注文内容確認', '確認'].includes(t);
}

function isResumeText(text) {
  const t = normalizeIncomingText(text);
  return ['続き', '再開', '続ける'].includes(t);
}

function isNotifyIdText(text) {
  const t = normalizeIncomingText(text);
  return t === '通知先ID';
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

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});
