const express = require('express');
const crypto = require('crypto');
const path = require('path');
const Stripe = require('stripe');

const app = express();

const PORT = process.env.PORT || 10000;
const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET || '';
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const RESERVATION_SAVE_URL = process.env.RESERVATION_SAVE_URL || '';
const STORE_NOTIFY_LINE_ID = process.env.STORE_NOTIFY_LINE_ID || '';
const LIFF_ID = process.env.LIFF_ID || '';

const APP_BASE_URL =
  process.env.APP_BASE_URL ||
  process.env.RENDER_EXTERNAL_URL ||
  process.env.PUBLIC_BASE_URL ||
  '';

const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';
const STRIPE_PAYMENT_METHOD_TYPES = String(
  process.env.STRIPE_PAYMENT_METHOD_TYPES || 'paypay'
)
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);

const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

const APP_VERSION = '2026-03-30-liiffix-17-stripe-01';

const STORE_NAME = 'かむらど';
const STORE_CODE = 'KMR';
const TIME_ZONE = 'Asia/Tokyo';
const BOOKABLE_DATE_COUNT = 31;
const ORDER_START_DATE = '2026-04-02';

const PENDING_REMINDER_MINUTES = Number(process.env.PENDING_REMINDER_MINUTES || 5);
const REMINDER_CRON_TOKEN = process.env.REMINDER_CRON_TOKEN || '';
const SAME_DAY_LEAD_MINUTES = Number(process.env.SAME_DAY_LEAD_MINUTES || 30);
const CHANGE_LIMIT_MINUTES = 30;

const BACK_ACTION = 'go_back';
const CANCEL_ACTION = 'cancel_reservation';
const BACK_DISPLAY_TEXT = '一つ前に戻る';
const CANCEL_DISPLAY_TEXT = 'キャンセルする';

const CHANGE_DATE_ACTION = 'change_date';
const CHANGE_TIME_ACTION = 'change_time';
const CHANGE_NAME_ACTION = 'change_name';
const CHANGE_PHONE_ACTION = 'change_phone';
const CHANGE_REVIEW_ACTION = 'change_review';
const CHANGE_CONFIRM_ACTION = 'change_confirm';
const CHANGE_ITEMS_ACTION = 'change_items';
const CHANGE_ADD_ITEMS_ACTION = 'change_add_items';
const CHANGE_CANCEL_REQUEST_ACTION = 'change_cancel_request';
const CHANGE_CANCEL_CONFIRM_RESERVATION_ACTION = 'change_cancel_confirm_reservation';
const DAILY_MENU_KEY = 'daily_menu';
const EXTRA_KARAAGE_KEY = 'extra_karaage';
const DRINK_KEY_PREFIX = 'drink_';

const DRINK_OPTIONS = [
  {
    key: 'irohasu',
    name: 'いろはす',
    price: 150,
    description: 'すっきり飲みやすいミネラルウォーター',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/e6b0b4-1.jpg'
  },
  {
    key: 'oolong',
    name: 'やかんの麦茶',
    price: 200,
    description: '食事と相性のいい定番ドリンク',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/518rlhbonql.jpg'
  },
  {
    key: 'cola',
    name: 'コーラ',
    price: 200,
    description: 'シュワッと爽快な人気ドリンク',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/e382b3e383bce383a9-1.jpg'
  },
  {
    key: 'cola_zero',
    name: 'コカコーラ・ゼロ',
    price: 200,
    description: 'ゼロシュガーゼロカロリー',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/mono62457659-240314-02.jpg'
  }
];

const DEFAULT_DAILY_MENU = {
  name: '日替わり弁当',
  price: 600,
  description: 'その日のお楽しみメニューです',
  imageUrl:
    'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/e38380e382a6e383b3e383ade383bce38389.jpeg',
  allowLargeRice: true
};

const MENUS = {
  karaage: {
    name: 'からあげ弁当',
    price: 700,
    description: 'ジューシーな唐揚げが人気の定番弁当',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/e59490e68f9ae38192.jpeg',
    allowLargeRice: true
  },
  shogayaki: {
    name: '生姜焼き弁当',
    price: 700,
    description: '香ばしく焼き上げたごはんが進む一品',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/5.png',
    allowLargeRice: true
  },
  chicken_nanban: {
    name: 'チキン南蛮弁当',
    price: 900,
    description: 'オリジナルタルタルが美味な至極の一品',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/3.png',
    allowLargeRice: true
  }
};

const EXTRA_MENUS = {
  [EXTRA_KARAAGE_KEY]: {
    name: '追加唐揚げ',
    price: 80,
    description: 'お弁当に追加できる唐揚げです（1個80円）',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/photo_2026-03-22_14-58-55.jpg',
    allowLargeRice: false
  }
};

const PICKUP_TIMES = [
  '11:30',
  '11:45',
  '12:00',
  '12:15',
  '12:30',
  '12:45',
  '13:00',
  '13:15',
  '13:30',
  '13:45',
  '14:00'
];

const sessions = new Map();

app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/liff-config', async (_req, res) => {
  try {
    const bookingConfig = await fetchBookingConfig();

    const rawAvailableDates =
      bookingConfig.ok && Array.isArray(bookingConfig.dates)
        ? bookingConfig.dates
            .map((item) => normalizeYmdDate(item?.date))
            .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
        : [];

    const availableDates = buildEffectiveAvailableDates(rawAvailableDates);

    const pickupTimesByDate = Object.fromEntries(
      availableDates.map((date) => [date, getAvailablePickupTimesForDate(date)])
    );

    res.json({
      liffId: LIFF_ID,
      bookableDateCount: BOOKABLE_DATE_COUNT,
      storeName: STORE_NAME,
      availableDates,
      pickupTimes: PICKUP_TIMES,
      pickupTimesByDate,
      sameDayLeadMinutes: SAME_DAY_LEAD_MINUTES,
      todayJst: getNowJstDateLabel(),
      version: APP_VERSION
    });
  } catch (error) {
    console.error('liff-config error:', error);
    res.json({
      liffId: LIFF_ID,
      bookableDateCount: BOOKABLE_DATE_COUNT,
      storeName: STORE_NAME,
      availableDates: [],
      pickupTimes: PICKUP_TIMES,
      pickupTimesByDate: {},
      sameDayLeadMinutes: SAME_DAY_LEAD_MINUTES,
      todayJst: getNowJstDateLabel(),
      version: APP_VERSION
    });
  }
});

app.get('/', (_req, res) => {
  res.status(200).send('ok');
});

app.get('/health', (_req, res) => {
  res.status(200).json({
    ok: true,
    version: APP_VERSION,
    file: __filename,
    cwd: process.cwd()
  });
});

app.get('/payments/success', async (req, res) => {
  const sessionId = String(req.query.session_id || '').trim();
  const reservationNo = String(req.query.reservationNo || '').trim();

  try {
    const payment = sessionId && stripe ? await getStripeCheckoutSummary(sessionId) : null;

    const title =
      payment?.paymentStatus === 'paid'
        ? 'お支払いが完了しました'
        : 'お支払い状況を確認中です';

    const lines = payment?.paymentStatus === 'paid'
      ? [
          '事前決済が完了しました。',
          'LINEにも予約確定メッセージをお送りします。',
          payment?.reservationNo || reservationNo
            ? `受付番号：${payment?.reservationNo || reservationNo}`
            : '',
          'この画面は閉じて大丈夫です。'
        ]
      : [
          '決済結果の反映を確認しています。',
          '数十秒ほどしてからLINEメッセージをご確認ください。',
          payment?.reservationNo || reservationNo
            ? `受付番号：${payment?.reservationNo || reservationNo}`
            : '',
          'この画面は閉じて大丈夫です。'
        ];

    res
      .status(200)
      .set('Content-Type', 'text/html; charset=utf-8')
      .send(buildSimpleHtmlPage(title, lines.filter(Boolean)));
  } catch (err) {
    console.error('payments/success error:', err);
    res
      .status(200)
      .set('Content-Type', 'text/html; charset=utf-8')
      .send(
        buildSimpleHtmlPage('お支払い状況を確認中です', [
          '決済結果の取得中にエラーが発生しました。',
          'LINEのメッセージをご確認ください。'
        ])
      );
  }
});

app.get('/payments/cancel', async (req, res) => {
  const reservationNo = String(req.query.reservationNo || '').trim();

  res
    .status(200)
    .set('Content-Type', 'text/html; charset=utf-8')
    .send(
      buildSimpleHtmlPage('お支払いは未完了です', [
        '決済はまだ完了していません。',
        reservationNo ? `受付番号：${reservationNo}` : '',
        '再度お支払いする場合は、LINEからもう一度ご案内してください。'
      ].filter(Boolean))
    );
});

app.post('/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  if (!stripe || !STRIPE_WEBHOOK_SECRET) {
    return res.status(503).send('stripe is not configured');
  }

  const signature = req.get('stripe-signature') || '';
  const rawBody = req.body instanceof Buffer ? req.body : Buffer.from('');

  let event;
  try {
    event = stripe.webhooks.constructEvent(rawBody, signature, STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error('stripe webhook signature error:', err);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  try {
    switch (event.type) {
      case 'checkout.session.completed':
      case 'checkout.session.async_payment_succeeded':
        await handleStripeCheckoutCompleted(event.data.object);
        break;
      case 'checkout.session.async_payment_failed':
        await handleStripeCheckoutAsyncFailed(event.data.object);
        break;
      case 'checkout.session.expired':
        await handleStripeCheckoutExpired(event.data.object);
        break;
      default:
        break;
    }

    return res.status(200).json({ received: true });
  } catch (err) {
    console.error('stripe webhook handler error:', err);
    return res.status(500).send('webhook handler error');
  }
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

function normalizeActionToken(value) {
  return String(value || '').trim().toLowerCase();
}

function getRichMenuIntentFromEvent(event) {
  if (!event) return '';

  if (event.type === 'message' && event.message?.type === 'text') {
    const text = normalizeWebhookText(event.message.text || '');

    if (isReservationViewText(text)) return 'view';
    if (isReservationChangeText(text)) return 'change';
    return '';
  }

  if (event.type === 'postback') {
    const data = parsePostbackData(event.postback?.data || '');
    const action = normalizeActionToken(data.action || data.mode || data.type || '');
    const displayText = normalizeWebhookText(event.postback?.displayText || '');

    if (
      [
        'reservation_view',
        'reservation_confirm',
        'view_reservation',
        'show_reservation',
        'begin_view'
      ].includes(action)
    ) {
      return 'view';
    }

    if (
      ['reservation_change', 'change_reservation', 'begin_change'].includes(action)
    ) {
      return 'change';
    }

    if (displayText) {
      if (isReservationViewText(displayText)) return 'view';
      if (isReservationChangeText(displayText)) return 'change';
    }
  }

  return '';
}

async function handleRichMenuEntry(event, replyToken, userId) {
  const intent = getRichMenuIntentFromEvent(event);

  if (!intent || !userId) return false;

  if (intent === 'view') {
    await startLineLoading(userId, 5);
    await sleep(900);
    await handleViewLatestReservation(replyToken, userId);
    return true;
  }

  if (intent === 'change') {
    await startLineLoading(userId, 5);
    await sleep(900);
    await beginReservationChangeFlow(replyToken, userId);
    return true;
  }

  return false;
}

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

  if (userId && (event.type === 'message' || event.type === 'postback')) {
    const handled = await handleRichMenuEntry(event, replyToken, userId);
    if (handled) return;
  }

  if (event.type === 'message' && event.message?.type === 'text') {
    const rawText = event.message.text || '';
    const text = normalizeWebhookText(rawText);

    console.log(`[INCOMING ${APP_VERSION}]`, JSON.stringify(rawText));

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

    if (text.includes('予約日時|')) {
      console.log(`[LIFF ROUTE HIT ${APP_VERSION}]`, JSON.stringify(text));

      const normalized = text.slice(text.indexOf('予約日時|'));
      const [, selectedDate = '', selectedTime = ''] = normalized.split('|');

      await handleSelectedDateTime(
        replyToken,
        userId,
        session,
        selectedDate,
        selectedTime
      );
      return;
    }

    if (isReservationStartText(text)) {
      await startLineLoading(userId, 5);
      await sleep(1200);
      await beginReservationFlow(replyToken, userId);
      return;
    }

    if (isReservationViewText(text)) {
      await startLineLoading(userId, 5);
      await sleep(900);
      await handleViewLatestReservation(replyToken, userId);
      return;
    }

    if (isReservationChangeText(text)) {
      await startLineLoading(userId, 5);
      await sleep(900);
      await beginReservationChangeFlow(replyToken, userId);
      return;
    }

    if (isResumeText(text)) {
      if (hasActiveSession(session)) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, buildResumeMessages(session));
        return;
      }

      await startLineLoading(userId, 5);
      await sleep(1200);
      await beginReservationFlow(replyToken, userId);
      return;
    }

    if (hasActiveSession(session) && isBackText(text)) {
      await handleBackAction(replyToken, userId, session);
      return;
    }

    if (hasActiveSession(session) && isCancelText(text)) {
      await handleCancelAction(replyToken, userId);
      return;
    }

    if (session?.step === 'waiting_date' && isYmdDate(text)) {
      await handleSelectedDate(replyToken, userId, session, text);
      return;
    }

    if (session?.step === 'change_waiting_date' && isYmdDate(text)) {
      await handleSelectedDate(replyToken, userId, session, text);
      return;
    }

    if (session?.step === 'waiting_qty' && isQtyText(text)) {
      const qty = parseQtyText(text);
      await handleQtySelection(replyToken, userId, session, qty);
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

      transitionSession(session, 'waiting_name');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        buildCartSummaryMessage(session),
        buildNameInputMessage()
      ]);
      return;
    }

    if (session?.step === 'waiting_name') {
      transitionSession(session, 'waiting_phone', { name: text });
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`ご予約名：${text}`),
        buildPhoneInputMessage()
      ]);
      return;
    }

    if (session?.step === 'waiting_phone') {
      const phone = normalizePhone(text);

      if (!isValidPhone(phone)) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage(
            '電話番号の形式が正しくありません。\n国内の電話番号を入力してください。\n例：09012345678 または 0312345678'
          ),
          buildPhoneInputMessage()
        ]);
        return;
      }

      transitionSession(session, 'confirm', { phone });
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`電話番号：${phone}`),
        buildConfirmMessage(session)
      ]);
      return;
    }

    if (session?.step === 'change_waiting_name') {
      transitionSession(session, 'change_menu', { name: text });
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`お名前を変更しました：${text}`),
        buildChangeCurrentSummaryMessage(session),
        buildChangeMenuMessage(session)
      ]);
      return;
    }

    if (session?.step === 'change_waiting_phone') {
      const phone = normalizePhone(text);

      if (!isValidPhone(phone)) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage(
            '電話番号の形式が正しくありません。\n国内の電話番号を入力してください。\n例：09012345678 または 0312345678'
          ),
          buildChangePhoneInputMessage()
        ]);
        return;
      }

      transitionSession(session, 'change_menu', { phone });
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`電話番号を変更しました：${phone}`),
        buildChangeCurrentSummaryMessage(session),
        buildChangeMenuMessage(session)
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
      await startLineLoading(userId, 5);
      await sleep(1200);
      await beginReservationFlo
