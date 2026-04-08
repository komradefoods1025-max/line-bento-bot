const express = require('express');
const crypto = require('crypto');
const path = require('path');

const app = express();

const PORT = process.env.PORT || 10000;
const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET || '';
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const RESERVATION_SAVE_URL = process.env.RESERVATION_SAVE_URL || '';
const STORE_NOTIFY_LINE_ID = process.env.STORE_NOTIFY_LINE_ID || '';
const STORE_NOTIFY_GROUP_ID = process.env.STORE_NOTIFY_GROUP_ID || '';
const LIFF_ID = process.env.LIFF_ID || '';

const APP_VERSION = '2026-04-04-group-notify-01';

const STORE_NAME = 'かむらど';
const STORE_CODE = 'KMR';
const TIME_ZONE = 'Asia/Tokyo';
const BOOKABLE_DATE_COUNT = 31;
const ORDER_START_DATE = '2026-04-02';
const MENU_IMAGE_URL = 'https://komradefoods1025-geskw.wpcomstaging.com/wp-content/uploads/2026/04/%E3%83%89%E3%83%AA%E3%83%B3%E3%82%AF%E3%83%A1%E3%83%8B%E3%83%A5%E3%83%BC%E2%91%A1.pdf.png';

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
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/e6b0b4-1.jpg',
    soldOut: false,
    visible: true
  },
  {
    key: 'oolong',
    name: 'やかんの麦茶',
    price: 200,
    description: '食事と相性のいい定番ドリンク',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/518rlhbonql.jpg',
    soldOut: false,
    visible: true
  },
  {
    key: 'cola',
    name: 'コーラ',
    price: 200,
    description: 'シュワッと爽快な人気ドリンク',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/e382b3e383bce383a9-1.jpg',
    soldOut: true,
    visible: true
  },
  {
    key: 'cola_zero',
    name: 'コカコーラ・ゼロ',
    price: 200,
    description: 'ゼロシュガーゼロカロリー',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/mono62457659-240314-02.jpg',
    soldOut: false,
    visible: true
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
const startTapLocks = new Map();
const START_TAP_LOCK_MS = 3000;
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

function getStoreNotifyTargetId() {
  return STORE_NOTIFY_GROUP_ID || STORE_NOTIFY_LINE_ID || '';
}

function buildNotifyTargetGuideMessage(sourceType, sourceId) {
  if (!sourceId) {
    return textMessage('通知先IDを取得できませんでした。');
  }

  if (sourceType === 'group') {
    return textMessage(
      `このグループの通知先IDはこちらです。\n\n${sourceId}\n\nこのIDを Render の STORE_NOTIFY_GROUP_ID に入れてください。`
    );
  }

  return textMessage(
    `現在の通知先IDはこちらです。\n\n${sourceId}\n\nこのIDを Render の STORE_NOTIFY_LINE_ID に入れてください。`
  );
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
    const action = normalizeActionToken(
      data.action || data.mode || data.type || ''
    );
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
      [
        'reservation_change',
        'change_reservation',
        'begin_change'
      ].includes(action)
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

  const sourceType = event.source?.type || '';
  const sourceId =
    sourceType === 'group'
      ? event.source?.groupId || ''
      : sourceType === 'room'
        ? event.source?.roomId || ''
        : event.source?.userId || '';

  const userId = event.source?.userId || null;
  const session = userId ? await loadSession(userId) : null;

  if (event.type === 'join' && sourceType === 'group') {
    console.log(`[GROUP JOIN ${APP_VERSION}]`, sourceId);

    await replyMessage(replyToken, [
      textMessage(
        '通知グループへの参加が完了しました。\nこのグループで「通知先ID」と送ると、Render に入れるグループIDを確認できます。'
      )
    ]);
    return;
  }

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
      const guideText =
        sourceType === 'group'
          ? `このグループの通知先IDはこちらです。\n\n${sourceId}\n\nこのIDを Render の STORE_NOTIFY_GROUP_ID に入れてください。`
          : `現在の通知先IDはこちらです。\n\n${sourceId}\n\nこのIDを Render の STORE_NOTIFY_LINE_ID に入れてください。`;

      await replyMessage(replyToken, [textMessage(guideText)]);
      return;
    }

    if (!userId) {
      await replyMessage(replyToken, [
        textMessage('予約は bot との1対1トークでご利用ください。')
      ]);
      return;
    }

    if (text === 'メニュー') {
      await replyMessage(replyToken, buildMenuImageMessages());
      return;
    }

    if (isStartReservationText(text) || isResetText(text)) {
      if (isStartTapLocked(userId)) {
        return;
      }

      if (hasActiveSession(session)) {
        await clearPendingSession(userId);
        clearSession(userId);
      }

      await startLineLoading(userId, 10);
      await replyMessage(replyToken, [buildBusyNoticeText('processing')]);
      await sleep(1200);
      await pushBeginReservationFlow(userId);
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
      if (!session?.items?.length) {
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
      if (isStartTapLocked(userId)) {
        return;
      }

      await startLineLoading(userId, 10);
      await replyMessage(replyToken, [buildBusyNoticeText('processing')]);
      await sleep(1500);
      await pushBeginReservationFlow(userId);
      return;
    }

    if (data.action === 'start_order_from_menu_image') {
      if (isStartTapLocked(userId)) {
        return;
      }

      if (hasActiveSession(session)) {
        await clearPendingSession(userId);
        clearSession(userId);
      }

      await startLineLoading(userId, 10);
      await replyMessage(replyToken, [buildBusyNoticeText('processing')]);
      await sleep(1200);
      await pushBeginReservationFlow(userId);
      return;
    }

    if (data.action === 'begin_change') {
      await beginReservationChangeFlow(replyToken, userId);
      return;
    }

    if (data.action === 'open_name_input') {
  if (!session) {
    await replyMessage(replyToken, [startGuideMessage()]);
    return;
  }

  transitionSession(session, 'waiting_name');
  await savePendingSession(userId, session);
  return;
}

  const isChangeFlow = session.step === 'change_waiting_name';

  if (!isChangeFlow) {
    transitionSession(session, 'waiting_name');
  }

  await savePendingSession(userId, session);

  await replyMessage(replyToken, [
    textMessage(
      isChangeFlow
        ? '変更後のお名前を入力してください👤'
        : 'お名前を入力してください👤'
    )
  ]);
  return;
}

if (data.action === 'open_phone_input') {
  if (!session) {
    await replyMessage(replyToken, [startGuideMessage()]);
    return;
  }

  const isChangeFlow = session.step === 'change_waiting_phone';

  if (!isChangeFlow) {
    transitionSession(session, 'waiting_phone');
  }

  await savePendingSession(userId, session);

  await replyMessage(replyToken, [
    textMessage(
      isChangeFlow
        ? '変更後の電話番号を入力してください📞\n例：09012345678'
        : '電話番号を入力してください📞\n例：09012345678'
    )
  ]);
  return;
}

    if (data.action === BACK_ACTION) {
      await handleBackAction(replyToken, userId, session);
      return;
    }

    if (data.action === CANCEL_ACTION) {
      await handleCancelAction(replyToken, userId);
      return;
    }

    if (data.action === CHANGE_DATE_ACTION) {
      transitionSession(session, 'change_waiting_date');
      await savePendingSession(userId, session);
      await replyMessage(replyToken, [
        textMessage('変更後の受取日を選んでください。'),
        createDateSelectMessage()
      ]);
      return;
    }

    if (data.action === CHANGE_TIME_ACTION) {
      transitionSession(session, 'change_waiting_time');
      await savePendingSession(userId, session);
      await replyMessage(replyToken, [
        textMessage(
          `変更後の受取時間を選んでください。\n現在の受取日：${formatDateWithWeekday(session.date)}`
        ),
        buildTimeMessage(session.date)
      ]);
      return;
    }

    if (data.action === CHANGE_NAME_ACTION) {
      transitionSession(session, 'change_waiting_name');
      await savePendingSession(userId, session);
      await replyMessage(replyToken, [buildChangeNameInputMessage()]);
      return;
    }

    if (data.action === CHANGE_PHONE_ACTION) {
      transitionSession(session, 'change_waiting_phone');
      await savePendingSession(userId, session);
      await replyMessage(replyToken, [buildChangePhoneInputMessage()]);
      return;
    }

    if (data.action === CHANGE_ADD_ITEMS_ACTION) {
      session.currentSelection = null;
      transitionSession(session, 'waiting_menu');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage('現在のご注文に商品を追加してください🍱'),
        ...buildMenuStepMessages(session)
      ]);
      return;
    }

    if (data.action === CHANGE_ITEMS_ACTION) {
      session.items = [];
      session.currentSelection = null;
      transitionSession(session, 'waiting_menu');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage('変更後のメニューを選んでください🍱\nいまのメニュー内容は一度リセットされます。'),
        ...buildMenuStepMessages(session)
      ]);
      return;
    }

    if (data.action === CHANGE_REVIEW_ACTION) {
      await savePendingSession(userId, session);
      await replyMessage(replyToken, [
        buildChangeCurrentSummaryMessage(session),
        buildChangeMenuMessage(session)
      ]);
      return;
    }

    if (data.action === CHANGE_CANCEL_REQUEST_ACTION) {
      await replyMessage(replyToken, [
        withNavQuickReply(
          {
            type: 'text',
            text: 'この予約自体をキャンセルしますか？\n※この操作でご予約は取り消しになります。',
            quickReply: {
              items: [
                quickPostbackItem(
                  'はい、キャンセルする',
                  `action=${CHANGE_CANCEL_CONFIRM_RESERVATION_ACTION}`,
                  'はい、キャンセルする'
                )
              ]
            }
          },
          { includeBack: true, includeCancel: false }
        )
      ]);
      return;
    }

    if (data.action === CHANGE_CANCEL_CONFIRM_RESERVATION_ACTION) {
      await handleReservationCancelConfirm(replyToken, userId, session);
      return;
    }

    if (data.action === CHANGE_CONFIRM_ACTION) {
      await handleReservationChangeConfirm(replyToken, userId, session);
      return;
    }

    if (data.action === 'pick_date') {
      const selectedDate = event.postback?.params?.date || '';
      await handleSelectedDate(replyToken, userId, session, selectedDate);
      return;
    }

    if (data.action === 'time') {
      const selectedTime = data.value || '';
      const availableTimes = getAvailablePickupTimesForDate(session.date);

      if (!availableTimes.includes(selectedTime)) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage(
            `受取時間をもう一度選んでください。\n本日は現在時刻の${SAME_DAY_LEAD_MINUTES}分後以降からご予約いただけます。`
          ),
          buildTimeMessage(session.date)
        ]);
        return;
      }

      if (session.flowType === 'change') {
        transitionSession(session, 'change_menu', { time: selectedTime });
        await savePendingSession(userId, session);

        await replyMessage(replyToken, [
          textMessage(`変更後の受取時間：${selectedTime}`),
          buildChangeCurrentSummaryMessage(session),
          buildChangeMenuMessage(session)
        ]);
        return;
      }

      transitionSession(session, 'waiting_menu', { time: selectedTime });
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

      if (menu.visible === false) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage(`申し訳ありません、${menu.name}は現在表示停止中です。`),
          ...buildMenuStepMessages(session)
        ]);
        return;
      }

      if (menu.soldOut) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage(`申し訳ありません、${menu.name}は売り切れです🙇‍♂️\n別の商品をお選びください。`),
          ...buildMenuStepMessages(session)
        ]);
        return;
      }

      session.currentSelection = {
        itemType: 'food',
        menuKey: data.item,
        menuName: menu.name,
        price: Number(menu.price || 0),
        riceSize: '',
        allowLargeRice: !!menu.allowLargeRice,
        drinkKey: '',
        drinkName: '',
        drinkPrice: 0
      };

      if (menu.allowLargeRice) {
        transitionSession(session, 'waiting_rice_size');
        await savePendingSession(userId, session);

        await replyMessage(replyToken, [
          textMessage(`ご注文商品：${menu.name}`),
          buildLargeRiceMessage(menu.name)
        ]);
        return;
      }

      if (canOfferDrinkForSelection(session.currentSelection)) {
        transitionSession(session, 'waiting_drink_confirm');
        await savePendingSession(userId, session);

        await replyMessage(replyToken, [
          textMessage(`ご注文商品：${menu.name}`),
          buildDrinkConfirmMessage(menu.name)
        ]);
        return;
      }

      transitionSession(session, 'waiting_qty');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`ご注文商品：${menu.name}`),
        buildQtyMessage(menu.name, 'food')
      ]);
      return;
    }

    if (data.action === 'drink') {
      const drink = resolveDrinkByKey(data.item || '');

      if (!drink || drink.visible === false) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('ドリンクが見つかりませんでした。'),
          ...buildMenuStepMessages(session)
        ]);
        return;
      }

      if (drink.soldOut) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage(`申し訳ありません、${drink.name}は売り切れです🙇‍♂️\n別のドリンクをお選びください。`),
          buildDrinkFlexMessage()
        ]);
        return;
      }

      if (session.step === 'waiting_drink_menu' && session.currentSelection) {
        session.currentSelection.drinkKey = `${DRINK_KEY_PREFIX}${drink.key}`;
        session.currentSelection.drinkName = drink.name;
        session.currentSelection.drinkPrice = Number(drink.price || 0);
        transitionSession(session, 'waiting_qty');
        await savePendingSession(userId, session);

        await replyMessage(replyToken, [
          textMessage(`ドリンク：${drink.name} を付けます。`),
          buildQtyMessage(
            getCurrentSelectionLabel(session.currentSelection),
            'food'
          )
        ]);
        return;
      }

      session.currentSelection = {
        itemType: 'drink',
        menuKey: `${DRINK_KEY_PREFIX}${drink.key}`,
        menuName: drink.name,
        price: Number(drink.price || 0),
        riceSize: '',
        allowLargeRice: false,
        drinkKey: '',
        drinkName: '',
        drinkPrice: 0
      };

      transitionSession(session, 'waiting_qty');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`ご注文商品：${drink.name}`),
        buildQtyMessage(drink.name, 'drink')
      ]);
      return;
    }

    if (data.action === 'rice_size') {
      if (!session.currentSelection) {
        session.step = 'waiting_menu';
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('もう一度商品を選んでください。'),
          ...buildMenuStepMessages(session)
        ]);
        return;
      }

      const riceSize = normalizeRiceSizeLabel(data.value) || '普通';
      session.currentSelection.riceSize = riceSize;

      const riceLabel = `ご飯${riceSize}`;

      if (canOfferDrinkForSelection(session.currentSelection)) {
        transitionSession(session, 'waiting_drink_confirm');
        await savePendingSession(userId, session);

        await replyMessage(replyToken, [
          textMessage(`${riceLabel}で承りました😊`),
          buildDrinkConfirmMessage(session.currentSelection.menuName)
        ]);
        return;
      }

      transitionSession(session, 'waiting_qty');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage(`${riceLabel}で承りました😊`),
        buildQtyMessage(
          getCurrentSelectionLabel(session.currentSelection),
          session.currentSelection.itemType || 'food'
        )
      ]);
      return;
    }

    if (data.action === 'drink_confirm') {
      if (!session.currentSelection) {
        session.step = 'waiting_menu';
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('もう一度商品を選んでください。'),
          ...buildMenuStepMessages(session)
        ]);
        return;
      }

      if (data.value === 'yes') {
        transitionSession(session, 'waiting_drink_menu');
        await savePendingSession(userId, session);

        await replyMessage(replyToken, [
          withNavQuickReply(
            textMessage('付けるドリンクを選んでください🥤'),
            { includeBack: true, includeCancel: true }
          ),
          buildDrinkFlexMessage()
        ]);
        return;
      }

      session.currentSelection.drinkKey = '';
      session.currentSelection.drinkName = '';
      session.currentSelection.drinkPrice = 0;
      transitionSession(session, 'waiting_qty');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        textMessage('ドリンクなしで承りました😊'),
        buildQtyMessage(
          getCurrentSelectionLabel(session.currentSelection),
          session.currentSelection.itemType || 'food'
        )
      ]);
      return;
    }

    if (data.action === 'qty') {
      const qty = Number(data.value || 0);
      await handleQtySelection(replyToken, userId, session, qty);
      return;
    }

    if (data.action === 'add_more') {
      transitionSession(session, 'waiting_menu');
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

      transitionSession(session, 'waiting_date');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        buildCartSummaryMessage(session),
        createDateSelectMessage()
      ]);
      return;
    }

    if (data.action === 'confirm') {
      if (!isReservationComplete(session)) {
        await startLineLoading(userId, 5);
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
          textMessage(
            `予約内容の保存でエラーが起きました。\n${saveResult.error}`
          )
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

function buildReservationCompleteMessage(reservation) {
  return textMessage(
    `ご予約ありがとうございます✨\n\n` +
      `受付番号：${reservation.reservationNo}\n` +
      `受取日：${formatDateWithWeekday(reservation.date)}\n` +
      `受取時間：${reservation.time}\n` +
      `ご注文内容：\n${formatOrderLines(reservation.items)}\n` +
      `合計個数：${reservation.totalQty}個\n` +
      `注文合計：¥${Number(reservation.total).toLocaleString('ja-JP')}\n` +
      `お名前：${reservation.name}様\n` +
      `電話番号：${reservation.phone}\n\n` +
      `ご来店をお待ちしています😊`
  );
}

async function notifyStoreByLine(reservation) {
  const notifyTo = getStoreNotifyTargetId();
  if (!notifyTo) return;

  const status = String(reservation?.status || '').trim();

  const title =
    status === 'キャンセル'
      ? '【店舗通知：予約キャンセル】'
      : status === '変更済み' || status === '変更'
        ? '【店舗通知：予約変更】'
        : '【店舗通知：新規ランチ予約】';

  await pushMessage(notifyTo, [
    textMessage(
      `${title}\n\n` +
        `受付番号：${reservation?.reservationNo || '-'}\n` +
        `受取日：${reservation?.date ? formatDateWithWeekday(reservation.date) : '-'}\n` +
        `受取時間：${reservation?.time || '-'}\n` +
        `ご注文内容：\n${formatOrderLines(reservation?.items || [])}\n` +
        `合計個数：${Number(reservation?.totalQty || 0)}個\n` +
        `注文合計：¥${Number(reservation?.total || 0).toLocaleString('ja-JP')}\n` +
        `お名前：${reservation?.name || '-'}\n` +
        `電話番号：${reservation?.phone || '-'}`
    )
  ]);
}

async function startLineLoading(userId, loadingSeconds = 5) {
  const seconds = Math.max(5, Math.min(60, Number(loadingSeconds) || 5));

  if (!userId || !CHANNEL_ACCESS_TOKEN) return;

  try {
    const response = await fetch('https://api.line.me/v2/bot/chat/loading/start', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${CHANNEL_ACCESS_TOKEN}`
      },
      body: JSON.stringify({
        chatId: userId,
        loadingSeconds: seconds
      })
    });

    if (!response.ok) {
      const text = await response.text();
      console.error('startLineLoading error:', response.status, text);
    }
  } catch (err) {
    console.error('startLineLoading error:', err);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isStartTapLocked(userId) {
  const now = Date.now();
  const last = Number(startTapLocks.get(userId) || 0);

  if (last && now - last < START_TAP_LOCK_MS) {
    return true;
  }

  startTapLocks.set(userId, now);
  return false;
}

function buildBusyNoticeText(kind = 'processing') {
  switch (kind) {
    case 'check':
      return textMessage(
        'ただいま確認をしております✨\n何も押さずにお待ちください🙇‍♂️'
      );
    case 'processing':
    default:
      return textMessage(
        'ただいま処理をしております✨\n何も押さずにお待ちください🙇‍♂️\n\n電波状況やご注文が多数\n入っている時は読み込みまでに\nお時間がかかる場合があります⌛️'
      );
  }
}

async function prepareReservationFlow(userId) {
  clearSession(userId);
  await clearPendingSession(userId);

  const session = getSession(userId);
  const bookingConfig = await fetchBookingConfig();
  const menuStatuses = await fetchMenuStatusesConfig();

  const rawAvailableDates =
    bookingConfig.ok && Array.isArray(bookingConfig.dates)
      ? bookingConfig.dates
          .map((item) => normalizeYmdDate(item?.date))
          .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
      : [];

  const availableDates = buildEffectiveAvailableDates(rawAvailableDates);

  if (!bookingConfig.ok || !availableDates.length) {
    return {
      ok: false,
      messages: [
        textMessage('現在ご案内できる営業日がありません。時間をおいてお試しください。')
      ]
    };
  }

  session.availableDateOptions = Array.isArray(bookingConfig.dates)
    ? bookingConfig.dates
    : [];
  session.availableDates = availableDates;
  session.menuStatuses = menuStatuses;
  session.history = [];
  session.step = 'waiting_menu';

  await savePendingSession(userId, session);

  return {
    ok: true,
    messages: buildMenuStepMessages(session)
  };
}

async function beginReservationFlow(replyToken, userId) {
  const result = await prepareReservationFlow(userId);
  await replyMessage(replyToken, result.messages);
}

async function pushBeginReservationFlow(userId) {
  const result = await prepareReservationFlow(userId);
  await pushMessage(userId, result.messages);
}

function createReservationStartMessage() {
  if (!LIFF_ID) {
    return withNavQuickReply(
      textMessage(
        `${STORE_NAME}のお弁当予約へようこそ🍱\n` +
        `受取日と受取時間を選んでください。\n\n` +
        `受取希望日を YYYY-MM-DD 形式で送ってください。\n` +
        `例：2026-04-02`
      ),
      { includeBack: true, includeCancel: true }
    );
  }

  return withNavQuickReply(
    {
      type: 'text',
      text:
        `${STORE_NAME}のお弁当予約へようこそ🍱\n` +
        `受取日と受取時間を選んでください。`,
      quickReply: {
        items: [
          {
            type: 'action',
            action: {
              type: 'uri',
              label: 'カレンダーを開く',
              uri: `https://liff.line.me/${LIFF_ID}`
            }
          }
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function createDateSelectMessage() {
  if (!LIFF_ID) {
    return withNavQuickReply(
      textMessage(
        '受取希望日を入力してください📅\n' +
        '例：2026-04-02'
      ),
      { includeBack: true, includeCancel: true }
    );
  }

  return withNavQuickReply(
    {
      type: 'text',
      text: 'カレンダーを開くから受け取り日時をご入力してください😊',
      quickReply: {
        items: [
          {
            type: 'action',
            action: {
              type: 'uri',
              label: 'カレンダーを開く',
              uri: `https://liff.line.me/${LIFF_ID}`
            }
          }
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}
function isYmdDate(text) {
  return /^\d{4}-\d{2}-\d{2}$/.test(normalizeYmdDate(text));
}

function normalizeYmdDate(text) {
  return String(text || '').trim();
}

function getNowJstDateLabel() {
  const parts = getJstParts();
  return `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)}`;
}

function normalizeWebhookText(text) {
  return String(text || '').replace(/\r/g, '').trim();
}

function normalizeIncomingText(text) {
  return normalizeWebhookText(text).replace(/\s+/g, ' ');
}

function parseQtyText(text) {
  const normalized = normalizeIncomingText(text);
  const match = normalized.match(/(\d+)/);
  if (!match) return 0;
  return Number(match[1]);
}

function isQtyText(text) {
  const qty = parseQtyText(text);
  return Number.isInteger(qty) && qty > 0;
}

function textMessage(text) {
  return {
    type: 'text',
    text
  };
}

function parsePostbackData(data) {
  const result = {};
  const params = new URLSearchParams(String(data || ''));
  for (const [key, value] of params.entries()) {
    result[key] = value;
  }
  return result;
}

function quickPostbackItem(label, data, displayText = label, options = {}) {
  const action = {
    type: 'postback',
    label,
    data,
    displayText
  };

  if (options.inputOption) {
    action.inputOption = options.inputOption;
  }

  if (typeof options.fillInText === 'string' && options.fillInText.length > 0) {
    action.fillInText = options.fillInText;
  }

  return {
    type: 'action',
    action
  };
}

function quickMessageItem(label, text = label) {
  return {
    type: 'action',
    action: {
      type: 'message',
      label,
      text
    }
  };
}

function withNavQuickReply(message, options = {}) {
  const {
    includeBack = true,
    includeCancel = true,
    prependItems = [],
    appendItems = []
  } = options;

  const existingItems = Array.isArray(message?.quickReply?.items)
    ? message.quickReply.items
    : [];

  const items = [
    ...prependItems,
    ...existingItems,
    ...(includeBack
      ? [
          quickPostbackItem(
            BACK_DISPLAY_TEXT,
            `action=${BACK_ACTION}`,
            BACK_DISPLAY_TEXT
          )
        ]
      : []),
    ...(includeCancel
      ? [
          quickPostbackItem(
            CANCEL_DISPLAY_TEXT,
            `action=${CANCEL_ACTION}`,
            CANCEL_DISPLAY_TEXT
          )
        ]
      : []),
    ...appendItems
  ];

  return {
    ...message,
    quickReply: {
      items
    }
  };
}

function createDateChip(date) {
  const weekday = getWeekdayJp(date);
  return `${date}（${weekday}）`;
}

function getWeekdayJp(dateText) {
  const date = new Date(`${dateText}T00:00:00+09:00`);
  const week = ['日', '月', '火', '水', '木', '金', '土'];
  return week[date.getUTCDay()];
}

function formatDateWithWeekday(dateText) {
  const date = normalizeYmdDate(dateText);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return String(dateText || '');

  const [, year, month, day] = date.match(/^(\d{4})-(\d{2})-(\d{2})$/) || [];
  return `${Number(year)}/${Number(month)}/${Number(day)}（${getWeekdayJp(date)}）`;
}

function getAvailablePickupTimesForDate(dateStr, now = new Date()) {
  const normalizedDate = normalizeYmdDate(dateStr);

  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedDate)) {
    return PICKUP_TIMES;
  }

  if (normalizedDate < ORDER_START_DATE) {
    return [];
  }

  const todayJst = getNowJstDateLabel(now);

  if (normalizedDate !== todayJst) {
    return PICKUP_TIMES;
  }

  const threshold = new Date(now.getTime() + SAME_DAY_LEAD_MINUTES * 60 * 1000);

  return PICKUP_TIMES.filter((time) => {
    const pickupDateTime = jstDateTimeToUtcDate(normalizedDate, time);
    return pickupDateTime.getTime() >= threshold.getTime();
  });
}

function getNowJstDateLabel(now = new Date()) {
  const parts = getJstParts(now);
  return `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)}`;
}

function filterAvailableDatesByPickupTime(dates, now = new Date()) {
  return (dates || []).filter((date) => getAvailablePickupTimesForDate(date, now).length > 0);
}

function buildEffectiveAvailableDates(rawDates, now = new Date()) {
  const normalized = (rawDates || [])
    .map((date) => normalizeYmdDate(date))
    .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
    .filter((date) => date >= ORDER_START_DATE);

  const mergedDateSet = new Set(normalized);
  const todayJst = getNowJstDateLabel(now);

  if (
    todayJst >= ORDER_START_DATE &&
    getAvailablePickupTimesForDate(todayJst, now).length > 0
  ) {
    mergedDateSet.add(todayJst);
  }

  return filterAvailableDatesByPickupTime(Array.from(mergedDateSet), now).sort();
}

function buildTimeMessage(dateText) {
  const availableTimes = getAvailablePickupTimesForDate(dateText);

  if (!availableTimes.length) {
    return withNavQuickReply(
      textMessage(
        `この日は選べる受取時間がありません。\n別の日付をお選びください。`
      ),
      { includeBack: true, includeCancel: true }
    );
  }

  return withNavQuickReply(
    {
      type: 'text',
      text: `${formatDateWithWeekday(dateText)} の受取時間を選んでください⏰`,
      quickReply: {
        items: availableTimes.map((time) =>
          quickPostbackItem(time, `action=time&value=${encodeURIComponent(time)}`, time)
        )
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function getMenuStatusMap(session) {
  return session?.menuStatuses && typeof session.menuStatuses === 'object'
    ? session.menuStatuses
    : {};
}

function applyStatusToMenu(menuKey, baseMenu, menuStatuses) {
  const override = menuStatuses?.[menuKey] || {};
  return {
    ...baseMenu,
    soldOut: override.soldOut === true || baseMenu?.soldOut === true,
    visible: override.visible !== false && baseMenu?.visible !== false
  };
}

function resolveDrinkByKey(drinkKey) {
  const normalizedKey = String(drinkKey || '').replace(new RegExp(`^${DRINK_KEY_PREFIX}`), '');
  const found = DRINK_OPTIONS.find((drink) => drink.key === normalizedKey);
  return found ? { ...found } : null;
}

function resolveMenuByKey(session, itemKey) {
  const key = String(itemKey || '');
  const statusMap = getMenuStatusMap(session);

  if (key === DAILY_MENU_KEY) {
    const daily = session?.dailyMenu || DEFAULT_DAILY_MENU;
    return applyStatusToMenu(DAILY_MENU_KEY, daily, statusMap);
  }

  if (EXTRA_MENUS[key]) {
    return applyStatusToMenu(key, EXTRA_MENUS[key], statusMap);
  }

  if (MENUS[key]) {
    return applyStatusToMenu(key, MENUS[key], statusMap);
  }

  if (key.startsWith(DRINK_KEY_PREFIX)) {
    const drink = resolveDrinkByKey(key);
    if (!drink) return null;
    return applyStatusToMenu(key, drink, statusMap);
  }

  return null;
}

function canOfferDrinkForSelection(selection) {
  if (!selection) return false;
  if (selection.itemType !== 'food') return false;
  return !String(selection.menuKey || '').startsWith(DRINK_KEY_PREFIX);
}

function getCurrentSelectionLabel(selection) {
  if (!selection) return '';

  const parts = [selection.menuName || ''];

  if (selection.riceSize) {
    parts.push(`ご飯${selection.riceSize}`);
  }

  if (selection.drinkName) {
    parts.push(`+ ${selection.drinkName}`);
  }

  return parts.filter(Boolean).join(' / ');
}

function buildMenuBubble(itemKey, menu) {
  const isSoldOut = menu?.soldOut === true;

  const buttonLabel = isSoldOut
    ? '売り切れ'
    : itemKey === EXTRA_KARAAGE_KEY
      ? '追加する'
      : 'この商品を選ぶ';

  const displayText = isSoldOut
    ? `${menu.name}は売り切れ`
    : itemKey === EXTRA_KARAAGE_KEY
      ? `${menu.name}を追加する`
      : `${menu.name}を選ぶ`;

  const bodyContents = [
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
  ];

  if (menu.allowLargeRice) {
    bodyContents.push({
      type: 'text',
      text: 'ご飯大盛り無料',
      size: 'xs',
      color: '#B45309',
      wrap: true
    });
  }

  if (isSoldOut) {
    bodyContents.push({
      type: 'text',
      text: '本日売り切れ',
      size: 'sm',
      weight: 'bold',
      color: '#DC2626',
      wrap: true
    });
  }

  return {
    type: 'bubble',
    hero: {
      type: 'image',
      url: menu.imageUrl,
      size: 'full',
      aspectRatio: '20:13',
      aspectMode: 'cover',
      action: {
        type: 'postback',
        label: buttonLabel,
        data: `action=menu&item=${encodeURIComponent(itemKey)}`,
        displayText
      }
    },
    body: {
      type: 'box',
      layout: 'vertical',
      spacing: 'sm',
      contents: bodyContents
    },
    footer: {
      type: 'box',
      layout: 'vertical',
      spacing: 'sm',
      contents: [
        {
          type: 'button',
          style: isSoldOut ? 'secondary' : 'primary',
          action: {
            type: 'postback',
            label: buttonLabel,
            data: `action=menu&item=${encodeURIComponent(itemKey)}`,
            displayText
          }
        }
      ]
    }
  };
}

function buildDrinkBubble(drink) {
  const isSoldOut = drink?.soldOut === true;

  const buttonLabel = isSoldOut ? '売り切れ' : 'このドリンクを選ぶ';
  const displayText = isSoldOut
    ? `${drink.name}は売り切れ`
    : `${drink.name}を選ぶ`;

  const bodyContents = [
    {
      type: 'text',
      text: drink.name,
      weight: 'bold',
      size: 'lg',
      wrap: true
    },
    {
      type: 'text',
      text: `¥${Number(drink.price).toLocaleString('ja-JP')}`,
      weight: 'bold',
      size: 'md',
      color: '#16A34A'
    },
    {
      type: 'text',
      text: drink.description || '',
      size: 'sm',
      color: '#666666',
      wrap: true
    }
  ];

  if (isSoldOut) {
    bodyContents.push({
      type: 'text',
      text: '本日売り切れ',
      size: 'sm',
      weight: 'bold',
      color: '#DC2626',
      wrap: true
    });
  }

  return {
    type: 'bubble',
    hero: {
      type: 'image',
      url: drink.imageUrl,
      size: 'full',
      aspectRatio: '20:13',
      aspectMode: 'cover',
      action: {
        type: 'postback',
        label: buttonLabel,
        data: `action=drink&item=${encodeURIComponent(drink.key)}`,
        displayText
      }
    },
    body: {
      type: 'box',
      layout: 'vertical',
      spacing: 'sm',
      contents: bodyContents
    },
    footer: {
      type: 'box',
      layout: 'vertical',
      spacing: 'sm',
      contents: [
        {
          type: 'button',
          style: isSoldOut ? 'secondary' : 'primary',
          action: {
            type: 'postback',
            label: buttonLabel,
            data: `action=drink&item=${encodeURIComponent(drink.key)}`,
            displayText
          }
        }
      ]
    }
  };
}

function buildMenuFlexMessage(session) {
  const bubbles = [];

  // 日替わりは一時的に非表示
  // const dailyMenu = resolveMenuByKey(session, DAILY_MENU_KEY);
  // if (dailyMenu && dailyMenu.visible !== false) {
  //   bubbles.push(buildMenuBubble(DAILY_MENU_KEY, dailyMenu));
  // }

  Object.entries(MENUS).forEach(([key]) => {
    const menu = resolveMenuByKey(session, key);
    if (menu && menu.visible !== false) {
      bubbles.push(buildMenuBubble(key, menu));
    }
  });

  Object.entries(EXTRA_MENUS).forEach(([key]) => {
    const menu = resolveMenuByKey(session, key);
    if (menu && menu.visible !== false) {
      bubbles.push(buildMenuBubble(key, menu));
    }
  });

  return {
    type: 'flex',
    altText: 'メニュー一覧',
    contents: {
      type: 'carousel',
      contents: bubbles
    }
  };
}

function buildDrinkFlexMessage() {
  const contents = DRINK_OPTIONS
    .filter((drink) => drink.visible !== false)
    .map((drink) => buildDrinkBubble(drink));

  return {
    type: 'flex',
    altText: 'ドリンク一覧',
    contents: {
      type: 'carousel',
      contents
    }
  };
}

function buildLargeRiceMessage(menuName) {
  return withNavQuickReply(
    {
      type: 'text',
      text: `${menuName}のご飯サイズを選んでください🍚`,
      quickReply: {
        items: [
          quickPostbackItem('小盛り', 'action=rice_size&value=small', '小盛り'),
          quickPostbackItem('普通', 'action=rice_size&value=normal', '普通'),
          quickPostbackItem('大盛り', 'action=rice_size&value=large', '大盛り')
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function buildDrinkConfirmMessage(menuName) {
  return withNavQuickReply(
    {
      type: 'text',
      text: `${menuName}にドリンクを付けますか？🥤`,
      quickReply: {
        items: [
          quickPostbackItem('付ける', 'action=drink_confirm&value=yes', 'ドリンクを付ける'),
          quickPostbackItem('付けない', 'action=drink_confirm&value=no', 'ドリンクは付けない')
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function buildQtyMessage(name, itemType = 'food') {
  return withNavQuickReply(
    {
      type: 'text',
      text:
        itemType === 'drink'
          ? `${name} の本数を選んでください。`
          : `${name} の個数を選んでください。`,
      quickReply: {
        items: [1, 2, 3, 4, 5].map((qty) =>
          quickPostbackItem(`${qty}`, `action=qty&value=${qty}`, `${qty}`)
        )
      }
    },
    { includeBack: true, includeCancel: true }
  );
}
function textMessage(text, quickReply) {
  return {
    type: 'text',
    text,
    ...(quickReply ? { quickReply: { items: quickReply } } : {})
  };
}

function quickPostbackItem(label, data, displayText = label, options = {}) {
  const action = {
    type: 'postback',
    label,
    data,
    displayText
  };

  if (options.inputOption) {
    action.inputOption = options.inputOption;
  }

  if (typeof options.fillInText === 'string' && options.fillInText.length > 0) {
    action.fillInText = options.fillInText;
  }

  return {
    type: 'action',
    action
  };
}

function formatPickupDateTimeForDisplay(dateStr, timeStr) {
  const date = String(dateStr || '').trim();
  const time = String(timeStr || '').trim();

  if (!date) return time || '';

  const m = date.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return `${date} ${time}`.trim();

  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);

  const dt = new Date(`${date}T00:00:00+09:00`);
  const weeks = ['日', '月', '火', '水', '木', '金', '土'];
  const week = Number.isNaN(dt.getTime()) ? '' : `（${weeks[dt.getDay()]}）`;

  return `${year}/${month}/${day}${week} ${time}`.trim();
}

function buildMenuImageMessages() {
  return [
    {
      type: 'image',
      originalContentUrl: MENU_IMAGE_URL,
      previewImageUrl: MENU_IMAGE_URL
    },
    {
      type: 'text',
      text: 'メニューはこちらです。\nご予約に進む場合は下のボタンを押してください。',
      quickReply: {
        items: [
          {
            type: 'action',
            action: {
              type: 'postback',
              label: '予約へ進む',
              data: 'action=start_order_from_menu_image',
              displayText: '予約へ進む'
            }
          }
        ]
      }
    }
  ];
}
function buildMenuStepMessages(session) {
  return [
    withNavQuickReply(
      textMessage('ご希望の商品を選んでください🍱'),
      { includeBack: true, includeCancel: true }
    ),
    buildMenuFlexMessage(session)
  ];
}

function formatFoodLines(items) {
  const foods = (items || []).filter((item) => {
    const menuKey = String(item?.menuKey || '');
    return !(item?.itemType === 'drink' || menuKey.startsWith(DRINK_KEY_PREFIX));
  });

  if (!foods.length) return 'なし';
  return formatOrderLines(foods);
}

function getLargeRiceQty(items) {
  return (items || []).reduce((sum, item) => {
    const riceSize = normalizeRiceSizeLabel(item?.riceSize);
    return riceSize === '大盛り' ? sum + Number(item?.qty || 0) : sum;
  }, 0);
}

function hasDrinkItems(items) {
  return (items || []).some((item) => {
    const menuKey = String(item?.menuKey || '');
    return (
      item?.itemType === 'drink' ||
      menuKey.startsWith(DRINK_KEY_PREFIX) ||
      !!item?.drinkName
    );
  });
}

function formatPhoneForDisplay(phone) {
  const value = String(phone || '').trim();
  return value || '未設定';
}
function normalizeRiceSizeLabel(value) {
  const map = {
    small: '小盛り',
    normal: '普通',
    large: '大盛り',
    '小盛り': '小盛り',
    '普通': '普通',
    '大盛り': '大盛り'
  };
  return map[String(value || '')] || '';
}

function buildNameInputMessage() {
  return withNavQuickReply(
    {
      type: 'text',
      text: 'ご予約名を入力してください👤\n受け取りされる方のお名前でお願いします。',
      quickReply: {
        items: [
          quickPostbackItem(
            '名前を入力する',
            'action=open_name_input',
            '名前を入力する',
            {
              inputOption: 'openKeyboard',
              fillInText: ' '
            }
          )
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function buildPhoneInputMessage() {
  return withNavQuickReply(
    textMessage('ご連絡先を入力してください📞\n例：09012345678'),
    { includeBack: true, includeCancel: true }
  );
}

function buildChangeNameInputMessage() {
  return withNavQuickReply(
    {
      type: 'text',
      text: '変更後のお名前をご入力してください👤',
      quickReply: {
        items: [
          quickPostbackItem(
            '名前を入力する',
            'action=open_name_input',
            '名前を入力する',
            {
              inputOption: 'openKeyboard',
              fillInText: ' '
            }
          )
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function buildChangePhoneInputMessage() {
  return withNavQuickReply(
    {
      type: 'text',
      text: '変更後の電話番号をご入力してください🤙\n例：09012345678',
      quickReply: {
        items: [
          quickPostbackItem(
            '電話番号を入力する',
            'action=open_phone_input',
            '電話番号を入力する',
            {
              inputOption: 'openKeyboard',
              fillInText: ' '
            }
          )
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function formatOrderLines(items) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) return 'なし';

  return rows
    .map((item) => {
      const parts = [`・${item.name || item.menuName || '-'}`];
      if (item.riceSize) parts.push(`（ご飯${item.riceSize}）`);
      if (item.drinkName) parts.push(` + ${item.drinkName}`);
      parts.push(` ×${Number(item.qty || 0)}`);
      return parts.join('');
    })
    .join('\n');
}
function getItemRiceSizeLabel(item) {
  const value = String(item?.riceSize || '').trim();

  if (value === 'small' || value === '小' || value === '小盛り') return '小盛り';
  if (value === 'large' || value === '大' || value === '大盛り' || value === 'yes') return '大盛り';
  if (value === 'normal' || value === '普通' || value === 'no') return '普通';

  return '';
}

function getBaseItemDisplayName(item) {
  if (!item) return '商品';

  const baseName =
    item.menuName ||
    item.name ||
    '商品';

  const riceSize = getItemRiceSizeLabel(item);

  if (riceSize) {
    return `${baseName}（ご飯${riceSize}）`;
  }

  return baseName;
}

function formatFoodLines(items) {
  const foods = (items || []).filter((item) => {
    const menuKey = String(item?.menuKey || '');
    const isDrinkItem =
      item?.itemType === 'drink' ||
      menuKey.startsWith(DRINK_KEY_PREFIX);
    return !isDrinkItem;
  });

  if (!foods.length) return 'なし';

  return foods
    .map((item) => `・${getBaseItemDisplayName(item)} ×${Number(item?.qty || 0)}個`)
    .join('\n');
}

function formatDrinkLines(items) {
  const drinkRows = [];

  (items || []).forEach((item) => {
    const menuKey = String(item?.menuKey || '');
    const qty = Number(item?.qty || 0);

    if (item?.itemType === 'drink' || menuKey.startsWith(DRINK_KEY_PREFIX)) {
      drinkRows.push(`・${item?.menuName || item?.name || 'ドリンク'} ×${qty}個`);
      return;
    }

    if (item?.drinkName) {
      drinkRows.push(`・${item.drinkName} ×${qty}個`);
    }
  });

  if (!drinkRows.length) return 'なし';
  return drinkRows.join('\n');
}

function getLargeRiceQty(items) {
  return (items || []).reduce((sum, item) => {
    const riceSize = getItemRiceSizeLabel(item);
    if (riceSize === '大盛り') {
      return sum + Number(item?.qty || 0);
    }
    return sum;
  }, 0);
}

function hasDrinkItems(items) {
  return (items || []).some((item) => {
    const menuKey = String(item?.menuKey || '');
    return (
      item?.itemType === 'drink' ||
      menuKey.startsWith(DRINK_KEY_PREFIX) ||
      !!item?.drinkName
    );
  });
}
function getCartTotalQty(items) {
  return (Array.isArray(items) ? items : []).reduce(
    (sum, item) => sum + Number(item.qty || 0),
    0
  );
}

function getCartTotalAmount(items) {
  return (Array.isArray(items) ? items : []).reduce((sum, item) => {
    const base = Number(item.price || 0) * Number(item.qty || 0);
    const drink = Number(item.drinkPrice || 0) * Number(item.qty || 0);
    return sum + base + drink;
  }, 0);
}

function buildCartSummaryMessage(session) {
  return textMessage(
    `ご注文内容はこちらです。\n\n` +
      `受取日：${formatDateWithWeekday(session.date)}\n` +
      `受取時間：${session.time}\n` +
      `ご注文内容：\n${formatOrderLines(session.items)}\n` +
      `合計個数：${getCartTotalQty(session.items)}個\n` +
      `注文合計：¥${getCartTotalAmount(session.items).toLocaleString('ja-JP')}`
  );
}

function buildConfirmMessage(session) {
  return withNavQuickReply(
    {
      type: 'text',
      text:
        `この内容で予約しますか？\n\n` +
        `受取日：${formatDateWithWeekday(session.date)}\n` +
        `受取時間：${session.time}\n` +
        `ご注文内容：\n${formatOrderLines(session.items)}\n` +
        `合計個数：${getCartTotalQty(session.items)}個\n` +
        `注文合計：¥${getCartTotalAmount(session.items).toLocaleString('ja-JP')}\n` +
        `お名前：${session.name}\n` +
        `電話番号：${session.phone}`,
      quickReply: {
        items: [
          quickPostbackItem('予約を確定する', 'action=confirm', '予約を確定する'),
          quickPostbackItem('注文を追加する', 'action=add_more', '注文を追加する')
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function buildChangeCurrentSummaryMessage(session) {
  return textMessage(
    `現在の予約内容です。\n\n` +
      `受取日：${formatDateWithWeekday(session.date)}\n` +
      `受取時間：${session.time}\n` +
      `ご注文内容：\n${formatOrderLines(session.items)}\n` +
      `合計個数：${getCartTotalQty(session.items)}個\n` +
      `注文合計：¥${getCartTotalAmount(session.items).toLocaleString('ja-JP')}\n` +
      `お名前：${session.name || '-'}\n` +
      `電話番号：${session.phone || '-'}`
  );
}

function safeChangeMenuText(value, fallback = '未設定') {
  const text = String(value || '').trim();
  return text || fallback;
}

function buildChangeItemsText(items) {
  if (!Array.isArray(items) || !items.length) return '未設定';

  return (
    `${formatOrderLines(items)}\n` +
    `合計 ${getCartTotalQty(items)}個 / ¥${Number(getCartTotalAmount(items)).toLocaleString('ja-JP')}`
  );
}

function buildChangeMenuRow(label, value, actionData, displayText) {
  return {
    type: 'box',
    layout: 'horizontal',
    spacing: 'md',
    paddingAll: '12px',
    margin: 'md',
    cornerRadius: '12px',
    borderWidth: '1px',
    borderColor: '#E5E7EB',
    backgroundColor: '#FFFFFF',
    action: {
      type: 'postback',
      label,
      data: actionData,
      displayText
    },
    contents: [
      {
        type: 'box',
        layout: 'vertical',
        flex: 1,
        spacing: 'xs',
        contents: [
          {
            type: 'text',
            text: label,
            size: 'sm',
            weight: 'bold',
            color: '#111111'
          },
          {
            type: 'text',
            text: value,
            size: 'sm',
            color: '#444444',
            wrap: true
          }
        ]
      },
      {
        type: 'box',
        layout: 'vertical',
        flex: 0,
        justifyContent: 'center',
        contents: [
          {
            type: 'text',
            text: '変更',
            size: 'sm',
            weight: 'bold',
            color: '#16A34A'
          }
        ]
      }
    ]
  };
}

function buildChangeMenuMessage(session) {
  return {
    type: 'flex',
    altText: '変更したい項目をお選びください',
    contents: {
      type: 'bubble',
      size: 'mega',
      body: {
        type: 'box',
        layout: 'vertical',
        spacing: 'sm',
        paddingAll: '16px',
        backgroundColor: '#FFFDF7',
        contents: [
          {
            type: 'text',
            text: '変更したい項目をお選びください🙋‍♀️',
            size: 'lg',
            weight: 'bold',
            wrap: true,
            color: '#111111'
          },
          {
            type: 'text',
            text: '現在の内容を確認しながら、変更したい項目をタップできます。',
            size: 'xs',
            color: '#666666',
            wrap: true,
            margin: 'sm'
          },
          {
            type: 'separator',
            margin: 'md'
          },
          buildChangeMenuRow(
            'メニュー追加',
            '現在の注文に商品を追加',
            `action=${CHANGE_ADD_ITEMS_ACTION}`,
            'メニューを追加'
          ),
          buildChangeMenuRow(
            'メニュー変更',
            buildChangeItemsText(session && session.items),
            `action=${CHANGE_ITEMS_ACTION}`,
            'メニューを変更'
          ),
          buildChangeMenuRow(
            '受取日',
            session && session.date ? formatDateWithWeekday(session.date) : '未設定',
            `action=${CHANGE_DATE_ACTION}`,
            '受取日を変更'
          ),
          buildChangeMenuRow(
            '受取時間',
            safeChangeMenuText(session && session.time, '未設定'),
            `action=${CHANGE_TIME_ACTION}`,
            '受取時間を変更'
          ),
          buildChangeMenuRow(
            'お名前',
            session && session.name ? `${session.name}様` : '未設定',
            `action=${CHANGE_NAME_ACTION}`,
            '名前を変更'
          ),
          buildChangeMenuRow(
            '電話番号',
            formatPhoneForDisplay(session && session.phone),
            `action=${CHANGE_PHONE_ACTION}`,
            '電話番号を変更'
          )
        ]
      },
      footer: {
        type: 'box',
        layout: 'vertical',
        spacing: 'sm',
        paddingAll: '16px',
        contents: [
          {
            type: 'button',
            style: 'secondary',
            height: 'sm',
            action: {
              type: 'postback',
              label: '変更内容確認',
              data: `action=${CHANGE_REVIEW_ACTION}`,
              displayText: '変更内容確認'
            }
          },
          {
            type: 'button',
            style: 'primary',
            height: 'sm',
            action: {
              type: 'postback',
              label: '変更確定',
              data: `action=${CHANGE_CONFIRM_ACTION}`,
              displayText: '変更確定'
            }
          },
          {
            type: 'button',
            style: 'secondary',
            height: 'sm',
            action: {
              type: 'postback',
              label: '予約をキャンセル',
              data: `action=${CHANGE_CANCEL_REQUEST_ACTION}`,
              displayText: '予約をキャンセル'
            }
          }
        ]
      }
    }
  };
}

function isReservationComplete(session) {
  return !!(
    session &&
    session.date &&
    session.time &&
    Array.isArray(session.items) &&
    session.items.length &&
    session.name &&
    session.phone
  );
}

function normalizePhone(phone) {
  return String(phone || '').replace(/[^0-9]/g, '');
}

function isValidPhone(phone) {
  const value = normalizePhone(phone);
  if (!/^0\d{9,10}$/.test(value)) return false;

  return /^(0[5789]0\d{8}|0\d{9,10})$/.test(value);
}

function createBaseSession(userId) {
  return {
    userId,
    flowType: 'new',
    step: '',
    date: '',
    time: '',
    items: [],
    currentSelection: null,
    name: '',
    phone: '',
    history: [],
    dailyMenu: { ...DEFAULT_DAILY_MENU },
    menuStatuses: {},
    availableDates: [],
    availableDateOptions: []
  };
}

function getSession(userId) {
  if (!sessions.has(userId)) {
    sessions.set(userId, createBaseSession(userId));
  }
  return sessions.get(userId);
}

function clearSession(userId) {
  sessions.delete(userId);
}

function cloneForHistory(session) {
  return {
    flowType: session.flowType,
    step: session.step,
    date: session.date,
    time: session.time,
    items: Array.isArray(session.items) ? session.items.map((item) => ({ ...item })) : [],
    currentSelection: session.currentSelection ? { ...session.currentSelection } : null,
    name: session.name,
    phone: session.phone
  };
}

function transitionSession(session, nextStep, updates = {}) {
  if (!session.history) session.history = [];
  session.history.push(cloneForHistory(session));

  Object.assign(session, updates || {});
  session.step = nextStep;
  return session;
}

function rollbackSession(session) {
  if (!session?.history?.length) return false;
  const prev = session.history.pop();
  Object.assign(session, prev);
  return true;
}

function hasActiveSession(session) {
  return !!(session && session.step);
}

async function savePendingSession(userId, session) {
  try {
    await savePendingOrder({
      userId,
      step: session.step,
      payload: JSON.stringify({
        flowType: session.flowType,
        date: session.date,
        time: session.time,
        items: session.items,
        currentSelection: session.currentSelection,
        name: session.name,
        phone: session.phone,
        history: session.history,
        dailyMenu: session.dailyMenu,
        menuStatuses: session.menuStatuses,
        availableDates: session.availableDates,
        availableDateOptions: session.availableDateOptions
      })
    });
  } catch (err) {
    console.error('savePendingSession error:', err);
  }
}

async function clearPendingSession(userId) {
  try {
    await clearPendingOrder(userId);
  } catch (err) {
    console.error('clearPendingSession error:', err);
  }
}

function restoreSessionFromPending(pending) {
  const payload = safeJsonParse(pending?.payload || '{}', {});
  const session = createBaseSession(pending?.userId || '');

  session.flowType = payload.flowType || 'new';
  session.step = pending?.step || payload.step || '';
  session.date = payload.date || '';
  session.time = payload.time || '';
  session.items = Array.isArray(payload.items) ? payload.items : [];
  session.currentSelection = payload.currentSelection || null;
  session.name = payload.name || '';
  session.phone = payload.phone || '';
  session.history = Array.isArray(payload.history) ? payload.history : [];
  session.dailyMenu = payload.dailyMenu || { ...DEFAULT_DAILY_MENU };
  session.menuStatuses = payload.menuStatuses || {};
  session.availableDates = Array.isArray(payload.availableDates) ? payload.availableDates : [];
  session.availableDateOptions = Array.isArray(payload.availableDateOptions)
    ? payload.availableDateOptions
    : [];

  return session;
}

function safeJsonParse(text, fallback = null) {
  try {
    return JSON.parse(text);
  } catch (_err) {
    return fallback;
  }
}

async function loadSession(userId) {
  if (!userId) return null;

  if (sessions.has(userId)) {
    return sessions.get(userId);
  }

  try {
    const pending = await getPendingOrder(userId);
    if (pending?.ok && pending.found) {
      const restored = restoreSessionFromPending(pending.order || pending.pending || pending.data || pending);
      sessions.set(userId, restored);
      return restored;
    }
  } catch (err) {
    console.error('loadSession error:', err);
  }

  const session = createBaseSession(userId);
  sessions.set(userId, session);
  return session;
}

function buildResumeMessages(session) {
  if (!session) return [startGuideMessage()];

  switch (session.step) {
    case 'waiting_date':
      return [createDateSelectMessage()];
    case 'waiting_time':
    case 'change_waiting_time':
      return [buildTimeMessage(session.date)];
    case 'waiting_menu':
      return buildMenuStepMessages(session);
    case 'waiting_rice_size':
      return [buildLargeRiceMessage(session.currentSelection?.menuName || '商品')];
    case 'waiting_drink_confirm':
      return [buildDrinkConfirmMessage(session.currentSelection?.menuName || '商品')];
    case 'waiting_drink_menu':
      return [
        withNavQuickReply(textMessage('付けるドリンクを選んでください🥤'), {
          includeBack: true,
          includeCancel: true
        }),
        buildDrinkFlexMessage()
      ];
    case 'waiting_qty':
      return [
        buildQtyMessage(
          getCurrentSelectionLabel(session.currentSelection),
          session.currentSelection?.itemType || 'food'
        )
      ];
    case 'waiting_name':
      return [buildCartSummaryMessage(session), buildNameInputMessage()];
    case 'waiting_phone':
      return [buildPhoneInputMessage()];
    case 'confirm':
      return [buildConfirmMessage(session)];
    case 'change_menu':
      return [buildChangeCurrentSummaryMessage(session), buildChangeMenuMessage(session)];
    case 'change_waiting_name':
      return [buildChangeNameInputMessage()];
    case 'change_waiting_phone':
      return [buildChangePhoneInputMessage()];
    case 'change_waiting_date':
      return [textMessage('変更後の受取日を選んでください。'), createDateSelectMessage()];
    default:
      return [startGuideMessage()];
  }
}

function startGuideMessage() {
  return withNavQuickReply(
    textMessage(
      `${STORE_NAME}です🍱\n` +
        '「予約」と送るとご予約を開始できます。\n' +
        '「予約を確認」で現在の予約確認、\n' +
        '「予約変更」で変更やキャンセルができます。'
    ),
    {
      includeBack: false,
      includeCancel: false,
      appendItems: [
        quickMessageItem('予約', '予約'),
        quickMessageItem('予約を確認', '予約を確認'),
        quickMessageItem('予約変更', '予約変更')
      ]
    }
  );
}

async function handleBackAction(replyToken, userId, session) {
  if (!session || !rollbackSession(session)) {
    await clearPendingSession(userId);
    clearSession(userId);
    await replyMessage(replyToken, [startGuideMessage()]);
    return;
  }

  await savePendingSession(userId, session);
  await replyMessage(replyToken, buildResumeMessages(session));
}

async function handleCancelAction(replyToken, userId) {
  clearSession(userId);
  await clearPendingSession(userId);
  await replyMessage(replyToken, [textMessage('ご予約操作をキャンセルしました。'), startGuideMessage()]);
}

async function handleSelectedDateTime(replyToken, userId, session, selectedDate, selectedTime) {
  const currentSession = session || getSession(userId);
  const normalizedDate = normalizeYmdDate(selectedDate);
  const normalizedTime = String(selectedTime || '').trim();

  const bookingConfig = await fetchBookingConfig();
  const menuStatuses = await fetchMenuStatusesConfig();

  const rawAvailableDates =
    bookingConfig.ok && Array.isArray(bookingConfig.dates)
      ? bookingConfig.dates
          .map((item) => normalizeYmdDate(item?.date))
          .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
      : [];

  const availableDates = buildEffectiveAvailableDates(rawAvailableDates);

  if (!availableDates.includes(normalizedDate)) {
    currentSession.availableDates = availableDates;
    currentSession.step = 'waiting_date';
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage('選択された日付は現在ご利用いただけません。もう一度お選びください。'),
      createDateSelectMessage()
    ]);
    return;
  }

  const availableTimes = getAvailablePickupTimesForDate(normalizedDate);

  if (!availableTimes.includes(normalizedTime)) {
    currentSession.date = normalizedDate;
    currentSession.availableDates = availableDates;
    currentSession.menuStatuses = menuStatuses;
    currentSession.step = 'waiting_time';
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage('その時間は選べません。もう一度お選びください。'),
      buildTimeMessage(normalizedDate)
    ]);
    return;
  }

  currentSession.flowType = 'new';
  currentSession.availableDates = availableDates;
  currentSession.menuStatuses = menuStatuses;
  currentSession.date = normalizedDate;
  currentSession.time = normalizedTime;
  currentSession.currentSelection = null;
  currentSession.step = 'waiting_name';

  const menuResult = await fetchDailyMenu(normalizedDate);
  currentSession.dailyMenu = menuResult?.ok && menuResult.menu
    ? {
        ...DEFAULT_DAILY_MENU,
        ...menuResult.menu,
        allowLargeRice: menuResult.menu.allowLargeRice !== false
      }
    : { ...DEFAULT_DAILY_MENU };

  await savePendingSession(userId, currentSession);

  await replyMessage(replyToken, [
    textMessage(
      `ご注文内容はこちらです。\n\n` +
        `受取日：${formatDateWithWeekday(normalizedDate)}\n` +
        `受取時間：${normalizedTime}\n` +
        `ご注文内容：\n${formatOrderLines(currentSession.items)}\n` +
        `合計個数：${getCartTotalQty(currentSession.items)}個\n` +
        `注文合計：¥${Number(getCartTotalAmount(currentSession.items)).toLocaleString('ja-JP')}`
    ),
    buildNameInputMessage()
  ]);
}
async function handleSelectedDate(replyToken, userId, session, selectedDate) {
  const currentSession = session || getSession(userId);
  const normalizedDate = normalizeYmdDate(selectedDate);

  const bookingConfig = await fetchBookingConfig();
  const rawAvailableDates =
    bookingConfig.ok && Array.isArray(bookingConfig.dates)
      ? bookingConfig.dates
          .map((item) => normalizeYmdDate(item?.date))
          .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
      : [];

  const availableDates = buildEffectiveAvailableDates(rawAvailableDates);

  if (!availableDates.includes(normalizedDate)) {
    currentSession.availableDates = availableDates;
    currentSession.step = currentSession.flowType === 'change' ? 'change_waiting_date' : 'waiting_date';
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage('その日は現在選択できません。もう一度お選びください。'),
      createDateSelectMessage()
    ]);
    return;
  }

  currentSession.availableDates = availableDates;
  currentSession.date = normalizedDate;

  const menuStatuses = await fetchMenuStatusesConfig();
  currentSession.menuStatuses = menuStatuses;

  const menuResult = await fetchDailyMenu(normalizedDate);
  currentSession.dailyMenu = menuResult?.ok && menuResult.menu
    ? {
        ...DEFAULT_DAILY_MENU,
        ...menuResult.menu,
        allowLargeRice: menuResult.menu.allowLargeRice !== false
      }
    : { ...DEFAULT_DAILY_MENU };

  if (currentSession.flowType === 'change') {
    transitionSession(currentSession, 'change_waiting_time', { date: normalizedDate });
  } else {
    transitionSession(currentSession, 'waiting_time', { date: normalizedDate });
  }

  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [buildTimeMessage(normalizedDate)]);
}

async function handleSelectedTime(replyToken, userId, session, selectedTime) {
  const currentSession = session || getSession(userId);
  const availableTimes = getAvailablePickupTimesForDate(currentSession.date);

  if (!availableTimes.includes(selectedTime)) {
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage(
        `受取時間をもう一度選んでください。\n本日は現在時刻の${SAME_DAY_LEAD_MINUTES}分後以降からご予約いただけます。`
      ),
      buildTimeMessage(currentSession.date)
    ]);
    return;
  }

  if (currentSession.flowType === 'change') {
    transitionSession(currentSession, 'change_menu', { time: selectedTime });
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage(`変更後の受取時間：${selectedTime}`),
      buildChangeCurrentSummaryMessage(currentSession),
      buildChangeMenuMessage(currentSession)
    ]);
    return;
  }

  transitionSession(currentSession, 'waiting_menu', { time: selectedTime });
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    textMessage(`受取時間：${selectedTime}`),
    ...buildMenuStepMessages(currentSession)
  ]);
}

async function handleMenuSelection(replyToken, userId, session, itemKey) {
  const currentSession = session || getSession(userId);
  const menu = resolveMenuByKey(currentSession, itemKey);

  if (!menu) {
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage('メニューが見つかりませんでした。'),
      ...buildMenuStepMessages(currentSession)
    ]);
    return;
  }

  if (menu.visible === false) {
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage(`申し訳ありません、${menu.name}は現在表示停止中です。`),
      ...buildMenuStepMessages(currentSession)
    ]);
    return;
  }

  if (menu.soldOut) {
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage(`申し訳ありません、${menu.name}は売り切れです🙇‍♂️\n別の商品をお選びください。`),
      ...buildMenuStepMessages(currentSession)
    ]);
    return;
  }

  currentSession.currentSelection = {
    itemType: itemKey.startsWith(DRINK_KEY_PREFIX) ? 'drink' : 'food',
    menuKey: itemKey,
    menuName: menu.name,
    price: Number(menu.price || 0),
    riceSize: '',
    allowLargeRice: !!menu.allowLargeRice,
    drinkKey: '',
    drinkName: '',
    drinkPrice: 0
  };

  if (currentSession.currentSelection.itemType === 'food' && menu.allowLargeRice) {
    transitionSession(currentSession, 'waiting_rice_size');
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage(`ご注文商品：${menu.name}`),
      buildLargeRiceMessage(menu.name)
    ]);
    return;
  }

  if (canOfferDrinkForSelection(currentSession.currentSelection)) {
    transitionSession(currentSession, 'waiting_drink_confirm');
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage(`ご注文商品：${menu.name}`),
      buildDrinkConfirmMessage(menu.name)
    ]);
    return;
  }

  transitionSession(currentSession, 'waiting_qty');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    textMessage(`ご注文商品：${menu.name}`),
    buildQtyMessage(menu.name, currentSession.currentSelection.itemType)
  ]);
}

async function handleDrinkSelection(replyToken, userId, session, drinkKey) {
  const currentSession = session || getSession(userId);
  const drink = resolveDrinkByKey(drinkKey);

  if (!drink || drink.visible === false) {
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage('ドリンクが見つかりませんでした。'),
      ...buildMenuStepMessages(currentSession)
    ]);
    return;
  }

  if (drink.soldOut) {
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage(`申し訳ありません、${drink.name}は売り切れです🙇‍♂️\n別のドリンクをお選びください。`),
      buildDrinkFlexMessage()
    ]);
    return;
  }

  if (currentSession.step === 'waiting_drink_menu' && currentSession.currentSelection) {
    currentSession.currentSelection.drinkKey = `${DRINK_KEY_PREFIX}${drink.key}`;
    currentSession.currentSelection.drinkName = drink.name;
    currentSession.currentSelection.drinkPrice = Number(drink.price || 0);
    transitionSession(currentSession, 'waiting_qty');
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage(`ドリンク：${drink.name} を付けます。`),
      buildQtyMessage(getCurrentSelectionLabel(currentSession.currentSelection), 'food')
    ]);
    return;
  }

  currentSession.currentSelection = {
    itemType: 'drink',
    menuKey: `${DRINK_KEY_PREFIX}${drink.key}`,
    menuName: drink.name,
    price: Number(drink.price || 0),
    riceSize: '',
    allowLargeRice: false,
    drinkKey: '',
    drinkName: '',
    drinkPrice: 0
  };

  transitionSession(currentSession, 'waiting_qty');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    textMessage(`ご注文商品：${drink.name}`),
    buildQtyMessage(drink.name, 'drink')
  ]);
}

async function handleQtySelection(replyToken, userId, session, qty) {
  const currentSession = session || getSession(userId);

  if (!currentSession.currentSelection) {
    transitionSession(currentSession, 'waiting_menu');
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage('もう一度商品を選んでください。'),
      ...buildMenuStepMessages(currentSession)
    ]);
    return;
  }

  const count = Number(qty || 0);
  if (!Number.isInteger(count) || count <= 0) {
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage('個数をもう一度選んでください。'),
      buildQtyMessage(
        getCurrentSelectionLabel(currentSession.currentSelection),
        currentSession.currentSelection.itemType || 'food'
      )
    ]);
    return;
  }

  const selection = {
    name: currentSession.currentSelection.menuName,
    menuKey: currentSession.currentSelection.menuKey,
    price: Number(currentSession.currentSelection.price || 0),
    qty: count,
    riceSize: currentSession.currentSelection.riceSize || '',
    drinkKey: currentSession.currentSelection.drinkKey || '',
    drinkName: currentSession.currentSelection.drinkName || '',
    drinkPrice: Number(currentSession.currentSelection.drinkPrice || 0)
  };

  currentSession.items.push(selection);
  currentSession.currentSelection = null;
  transitionSession(currentSession, 'waiting_menu');
  await savePendingSession(userId, currentSession);

  await replyMessage(replyToken, [
    textMessage(
      `${selection.name}${selection.riceSize ? `（ご飯${selection.riceSize}）` : ''}${selection.drinkName ? ` + ${selection.drinkName}` : ''} を ${count}個 追加しました。`
    ),
    withNavQuickReply(
      {
        type: 'text',
        text: '続けて商品を追加しますか？注文確認へ進みますか？',
        quickReply: {
          items: [
            quickPostbackItem('商品を追加する', 'action=add_more', '商品を追加する'),
            quickPostbackItem('注文確認へ進む', 'action=review_order', '注文確認へ進む')
          ]
        }
      },
      { includeBack: true, includeCancel: true }
    )
  ]);
}

async function handleReviewOrder(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  if (!currentSession.items.length) {
    await savePendingSession(userId, currentSession);
    await replyMessage(replyToken, [
      textMessage('まだ商品が入っていません。'),
      ...buildMenuStepMessages(currentSession)
    ]);
    return;
  }

  transitionSession(currentSession, 'waiting_name');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    buildCartSummaryMessage(currentSession),
    buildNameInputMessage()
  ]);
}

async function handleOrderConfirm(replyToken, userId, session) {
  const currentSession = session || getSession(userId);

  if (!isReservationComplete(currentSession)) {
    await startLineLoading(userId, 5);
    await beginReservationFlow(replyToken, userId);
    return;
  }

  const reservation = {
    reservationNo: createReservationNo(),
    userId,
    date: currentSession.date,
    time: currentSession.time,
    items: currentSession.items.map((item) => ({ ...item })),
    itemCount: currentSession.items.length,
    totalQty: getCartTotalQty(currentSession.items),
    total: getCartTotalAmount(currentSession.items),
    name: currentSession.name,
    phone: currentSession.phone,
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

  await replyMessage(replyToken, [buildReservationCompleteMessage(reservation)]);
}

function getMinutesUntilPickup(reservation, now = new Date()) {
  const date = normalizeYmdDate(reservation?.date || '');
  const time = String(reservation?.time || '').trim();

  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;
  if (!/^\d{2}:\d{2}$/.test(time)) return null;

  const pickupAt = jstDateTimeToUtcDate(date, time);
  return Math.floor((pickupAt.getTime() - now.getTime()) / 60000);
}

function isReservationChangeLocked(reservation, now = new Date()) {
  const minutes = getMinutesUntilPickup(reservation, now);
  if (minutes == null) return false;
  return minutes < CHANGE_LIMIT_MINUTES;
}

function buildReservationChangeLockedMessage(reservation) {
  return textMessage(
    `こちらの予約は受取時刻の${CHANGE_LIMIT_MINUTES}分前を過ぎているため、LINEからは変更できません。\n` +
      `受取日：${formatDateWithWeekday(reservation.date)}\n` +
      `受取時間：${reservation.time}\n\n` +
      '店舗へ直接ご連絡ください。'
  );
}

function buildLatestReservationMessage(reservation) {
  const items = Array.isArray(reservation?.items) ? reservation.items : [];
  const totalQty =
    reservation?.totalQty != null && reservation.totalQty !== ''
      ? Number(reservation.totalQty)
      : getCartTotalQty(items);
  const totalAmount =
    reservation?.total != null && reservation.total !== ''
      ? Number(reservation.total)
      : getCartTotalAmount(items);

  const orderText =
    reservation?.orderLines && String(reservation.orderLines).trim()
      ? String(reservation.orderLines)
      : formatOrderLines(items);

  return textMessage(
    `現在のご予約内容です📋\n\n` +
      `受付番号：${reservation?.reservationNo || '-'}\n` +
      `受取日：${reservation?.date ? formatDateWithWeekday(reservation.date) : '-'}\n` +
      `受取時間：${reservation?.time || '-'}\n` +
      `ご注文内容：\n${orderText}\n` +
      `合計個数：${totalQty}個\n` +
      `注文合計：¥${Number(totalAmount).toLocaleString('ja-JP')}\n` +
      `お名前：${reservation?.name || '-'}様\n` +
      `電話番号：${formatPhoneForDisplay(reservation?.phone || '')}\n` +
      `ステータス：${reservation?.status || '受付済み'}`
  );
}

async function handleViewLatestReservation(replyToken, userId) {
  const result = await fetchLatestReservation(userId);

  if (!result.ok) {
    await replyMessage(replyToken, [
      textMessage(`予約内容の取得でエラーが起きました。\n${result.error || 'unknown error'}`)
    ]);
    return;
  }

  if (!result.found) {
    await replyMessage(replyToken, [textMessage('現在確認できるご予約がありません。')]);
    return;
  }

  await replyMessage(replyToken, [
    buildLatestReservationMessage(result.reservation),
    withNavQuickReply(
      {
        type: 'text',
        text: '予約変更する場合は下のボタンから進めます。',
        quickReply: {
          items: [quickPostbackItem('予約変更', 'action=begin_change', '予約変更')]
        }
      },
      { includeBack: false, includeCancel: false }
    )
  ]);
}

async function beginReservationChangeFlow(replyToken, userId) {
  const latest = await fetchLatestReservation(userId);

  if (!latest.ok || !latest.found) {
    await replyMessage(replyToken, [
      textMessage('変更できる予約が見つかりませんでした。'),
      startGuideMessage()
    ]);
    return;
  }

  const reservation = latest.reservation;

  if (String(reservation.status || '') === 'キャンセル') {
    await replyMessage(replyToken, [
      textMessage('この予約はすでにキャンセル済みです。'),
      startGuideMessage()
    ]);
    return;
  }

  if (isReservationChangeLocked(reservation)) {
    await replyMessage(replyToken, [buildReservationChangeLockedMessage(reservation)]);
    return;
  }

  clearSession(userId);
  await clearPendingSession(userId);

  const session = getSession(userId);
  session.flowType = 'change';
  session.step = 'change_menu';
  session.date = reservation.date || '';
  session.time = reservation.time || '';
  session.items = Array.isArray(reservation.items) ? reservation.items.map((item) => ({ ...item })) : [];
  session.currentSelection = null;
  session.name = reservation.name || '';
  session.phone = reservation.phone || '';
  session.latestReservation = reservation;
  session.latestReservationNo = reservation.reservationNo || '';
  session.history = [];

  const bookingConfig = await fetchBookingConfig();
  const rawAvailableDates =
    bookingConfig.ok && Array.isArray(bookingConfig.dates)
      ? bookingConfig.dates
          .map((item) => normalizeYmdDate(item?.date))
          .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
      : [];
  session.availableDates = buildEffectiveAvailableDates(rawAvailableDates);
  session.menuStatuses = await fetchMenuStatusesConfig();

  const menuResult = await fetchDailyMenu(session.date);
  session.dailyMenu = menuResult?.ok && menuResult.menu
    ? {
        ...DEFAULT_DAILY_MENU,
        ...menuResult.menu,
        allowLargeRice: menuResult.menu.allowLargeRice !== false
      }
    : { ...DEFAULT_DAILY_MENU };

  await savePendingSession(userId, session);

  await replyMessage(replyToken, [
    buildChangeCurrentSummaryMessage(session),
    buildChangeMenuMessage(session)
  ]);
}

async function handleChangeDateAction(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  transitionSession(currentSession, 'change_waiting_date');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    textMessage('変更後の受取日を選んでください。'),
    createDateSelectMessage()
  ]);
}

async function handleChangeTimeAction(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  transitionSession(currentSession, 'change_waiting_time');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    textMessage(
      `変更後の受取時間を選んでください。\n現在の受取日：${formatDateWithWeekday(currentSession.date)}`
    ),
    buildTimeMessage(currentSession.date)
  ]);
}

async function handleChangeNameAction(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  transitionSession(currentSession, 'change_waiting_name');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [buildChangeNameInputMessage()]);
}

async function handleChangePhoneAction(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  transitionSession(currentSession, 'change_waiting_phone');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [buildChangePhoneInputMessage()]);
}

async function handleChangeItemsAction(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  currentSession.items = [];
  currentSession.currentSelection = null;
  transitionSession(currentSession, 'waiting_menu');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    textMessage('変更後のメニューを選んでください🍱\nいまのメニュー内容は一度リセットされます。'),
    ...buildMenuStepMessages(currentSession)
  ]);
}

async function handleChangeAddItemsAction(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  currentSession.currentSelection = null;
  transitionSession(currentSession, 'waiting_menu');
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    textMessage('現在のご注文に商品を追加してください🍱'),
    ...buildMenuStepMessages(currentSession)
  ]);
}

async function handleChangeReviewAction(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  await savePendingSession(userId, currentSession);
  await replyMessage(replyToken, [
    buildChangeCurrentSummaryMessage(currentSession),
    buildChangeMenuMessage(currentSession)
  ]);
}

async function handleReservationChangeConfirm(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  const reservationNo = currentSession.latestReservationNo || currentSession.latestReservation?.reservationNo || '';

  if (!reservationNo) {
    await replyMessage(replyToken, [textMessage('変更対象の予約番号が見つかりませんでした。')]);
    return;
  }

  const changedReservation = {
    reservationNo,
    userId,
    date: currentSession.date,
    time: currentSession.time,
    items: currentSession.items.map((item) => ({ ...item })),
    itemCount: currentSession.items.length,
    totalQty: getCartTotalQty(currentSession.items),
    total: getCartTotalAmount(currentSession.items),
    name: currentSession.name,
    phone: currentSession.phone,
    status: '変更済み',
    updatedAt: getJstDateTimeLabel()
  };

  const result = await changeReservationInSheet(changedReservation);
  if (!result.ok) {
    await replyMessage(replyToken, [
      textMessage(`予約変更の保存でエラーが起きました。\n${result.error}`)
    ]);
    return;
  }

  notifyStoreByLine(changedReservation).catch((err) =>
    console.error('store line notify error:', err)
  );

  clearSession(userId);
  await clearPendingSession(userId);

  await replyMessage(replyToken, [
    textMessage(
      `ご予約を変更しました。\n\n` +
        `受付番号：${changedReservation.reservationNo}\n` +
        `受取日：${formatDateWithWeekday(changedReservation.date)}\n` +
        `受取時間：${changedReservation.time}\n` +
        `ご注文内容：\n${formatOrderLines(changedReservation.items)}\n` +
        `合計個数：${changedReservation.totalQty}個\n` +
        `注文合計：¥${Number(changedReservation.total).toLocaleString('ja-JP')}\n` +
        `お名前：${changedReservation.name}\n` +
        `電話番号：${changedReservation.phone}`
    )
  ]);
}

async function handleReservationCancelConfirm(replyToken, userId, session) {
  const currentSession = session || getSession(userId);
  const reservationNo = currentSession.latestReservationNo || currentSession.latestReservation?.reservationNo || '';

  if (!reservationNo) {
    await replyMessage(replyToken, [textMessage('キャンセル対象の予約番号が見つかりませんでした。')]);
    return;
  }

  const cancelReservation = {
    reservationNo,
    userId,
    date: currentSession.date,
    time: currentSession.time,
    items: currentSession.items.map((item) => ({ ...item })),
    itemCount: currentSession.items.length,
    totalQty: getCartTotalQty(currentSession.items),
    total: getCartTotalAmount(currentSession.items),
    name: currentSession.name,
    phone: currentSession.phone,
    status: 'キャンセル',
    updatedAt: getJstDateTimeLabel()
  };

  const result = await cancelReservationInSheet(cancelReservation);
  if (!result.ok) {
    await replyMessage(replyToken, [
      textMessage(`予約キャンセルでエラーが起きました。\n${result.error}`)
    ]);
    return;
  }

  notifyStoreByLine(cancelReservation).catch((err) =>
    console.error('store line notify error:', err)
  );

  clearSession(userId);
  await clearPendingSession(userId);

  await replyMessage(replyToken, [
    textMessage(
      `ご予約をキャンセルしました。\n\n` +
        `受付番号：${cancelReservation.reservationNo}\n` +
        `受取日：${formatDateWithWeekday(cancelReservation.date)}\n` +
        `受取時間：${cancelReservation.time}`
    )
  ]);
}

async function fetchBookingConfig() {
  try {
    const url = buildReservationApiUrl({ action: 'getBookingConfig' });
    const response = await fetch(url);
    const json = await response.json();
    return json;
  } catch (err) {
    console.error('fetchBookingConfig error:', err);
    return { ok: false, error: err.message || String(err), dates: [] };
  }
}

async function fetchDailyMenu(date) {
  try {
    const url = buildReservationApiUrl({ action: 'getDailyMenu', date });
    const response = await fetch(url);
    const json = await response.json();
    return json;
  } catch (err) {
    console.error('fetchDailyMenu error:', err);
    return { ok: false, error: err.message || String(err), menu: null };
  }
}

async function fetchMenuStatusesConfig() {
  try {
    const url = buildReservationApiUrl({ action: 'getMenuStatuses' });
    const response = await fetch(url);
    const json = await response.json();
    return json?.ok && json.statuses ? json.statuses : {};
  } catch (err) {
    console.error('fetchMenuStatusesConfig error:', err);
    return {};
  }
}

async function saveReservationToSheet(reservation) {
  try {
    const items = Array.isArray(reservation.items) ? reservation.items : [];
    const orderLines = formatOrderLines(items);
    const foodLines = formatFoodLines(items);
    const drinkLines = formatDrinkLines(items);
    const largeRiceQty = getLargeRiceQty(items);
    const hasDrink = hasDrinkItems(items);

    const url = buildReservationApiUrl({
      action: 'saveReservation',
      reservationNo: reservation.reservationNo,
      userId: reservation.userId,
      date: reservation.date,
      time: reservation.time,
      name: reservation.name,
      phone: reservation.phone,
      status: reservation.status || '受付済み',
      createdAt: reservation.createdAt || '',
      itemCount: String(reservation.itemCount || 0),
      totalQty: String(reservation.totalQty || 0),
      total: String(reservation.total || 0),
      itemsJson: JSON.stringify(items),
      orderLines,
      foodLines,
      drinkLines,
      hasDrink: hasDrink ? 'yes' : 'no',
      hasLargeRice: largeRiceQty > 0 ? 'yes' : 'no',
      largeRiceQty: String(largeRiceQty),
      notifyMail: 'yes',
      notifyType: 'new'
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text };

    const json = JSON.parse(text);
    return json.ok
      ? { ok: true }
      : { ok: false, error: json.error || 'save error' };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function changeReservationInSheet(reservation) {
  try {
    const items = Array.isArray(reservation.items) ? reservation.items : [];
    const orderLines = formatOrderLines(items);
    const foodLines = formatFoodLines(items);
    const drinkLines = formatDrinkLines(items);
    const largeRiceQty = getLargeRiceQty(items);
    const hasDrink = hasDrinkItems(items);

    const url = buildReservationApiUrl({
      action: 'updateReservation',
      reservationNo: reservation.reservationNo,
      userId: reservation.userId,
      date: reservation.date,
      time: reservation.time,
      name: reservation.name,
      phone: reservation.phone,
      status: reservation.status || '変更済み',
      updatedAt: reservation.updatedAt || '',
      itemCount: String(reservation.itemCount || 0),
      totalQty: String(reservation.totalQty || 0),
      total: String(reservation.total || 0),
      itemsJson: JSON.stringify(items),
      orderLines,
      foodLines,
      drinkLines,
      hasDrink: hasDrink ? 'yes' : 'no',
      hasLargeRice: largeRiceQty > 0 ? 'yes' : 'no',
      largeRiceQty: String(largeRiceQty),
      notifyMail: 'yes',
      notifyType: 'change'
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text };

    const json = JSON.parse(text);
    return json.ok
      ? { ok: true }
      : { ok: false, error: json.error || 'update error' };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function cancelReservationInSheet(reservation) {
  try {
    const items = Array.isArray(reservation.items) ? reservation.items : [];
    const orderLines = formatOrderLines(items);
    const foodLines = formatFoodLines(items);
    const drinkLines = formatDrinkLines(items);
    const largeRiceQty = getLargeRiceQty(items);
    const hasDrink = hasDrinkItems(items);

    const url = buildReservationApiUrl({
      action: 'cancelReservation',
      reservationNo: reservation.reservationNo,
      userId: reservation.userId,
      date: reservation.date,
      time: reservation.time,
      name: reservation.name,
      phone: reservation.phone,
      status: 'キャンセル',
      canceledAt: reservation.canceledAt || '',
      updatedAt: reservation.updatedAt || '',
      itemCount: String(reservation.itemCount || 0),
      totalQty: String(reservation.totalQty || 0),
      total: String(reservation.total || 0),
      itemsJson: JSON.stringify(items),
      orderLines,
      foodLines,
      drinkLines,
      hasDrink: hasDrink ? 'yes' : 'no',
      hasLargeRice: largeRiceQty > 0 ? 'yes' : 'no',
      largeRiceQty: String(largeRiceQty),
      notifyMail: 'yes',
      notifyType: 'cancel'
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, error: text };

    const json = JSON.parse(text);
    return json.ok
      ? { ok: true }
      : { ok: false, error: json.error || 'cancel error' };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function fetchLatestReservation(userId) {
  try {
    const url = buildReservationApiUrl({ action: 'getLatestReservation', userId });
    const response = await fetch(url);
    const json = await response.json();

    if (!json?.ok || !json?.found) {
      return { ok: true, found: false };
    }

    return {
      ok: true,
      found: true,
      reservation: reservationFromApiRow(json.reservation || json.data || json.row || {})
    };
  } catch (err) {
    console.error('fetchLatestReservation error:', err);
    return { ok: false, error: err.message || String(err), found: false };
  }
}

function reservationFromApiRow(row) {
  const raw = row || {};

  const items =
    Array.isArray(raw.items)
      ? raw.items
      : safeJsonParse(
          raw.itemsJson ||
            raw.items_json ||
            raw.orderItemsJson ||
            raw.order_items_json ||
            '[]',
          []
        );

  const dateValue =
    raw.date ||
    raw.pickupDate ||
    raw.pickup_date ||
    raw.reservationDate ||
    raw.reservation_date ||
    '';

  const timeValue =
    raw.time ||
    raw.pickupTime ||
    raw.pickup_time ||
    raw.reservationTime ||
    raw.reservation_time ||
    '';

  return {
    reservationNo:
      raw.reservationNo ||
      raw.reservation_no ||
      raw.no ||
      raw.id ||
      '',
    userId:
      raw.userId ||
      raw.user_id ||
      '',
    date: normalizeYmdDate(dateValue),
    time: String(timeValue || '').trim(),
    items: Array.isArray(items) ? items : [],
    itemCount: Number(raw.itemCount || raw.item_count || 0),
    totalQty: Number(raw.totalQty || raw.total_qty || 0),
    total: Number(raw.total || raw.amount || 0),
    name: raw.name || '',
    phone: String(raw.phone || raw.tel || raw.telephone || ''),
    status: raw.status || '受付済み',
    createdAt: raw.createdAt || raw.created_at || '',
    updatedAt: raw.updatedAt || raw.updated_at || ''
  };
}

async function savePendingOrder(data) {
  try {
    const response = await fetch(RESERVATION_SAVE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'savePendingOrder', ...data })
    });
    return await response.json();
  } catch (err) {
    console.error('savePendingOrder error:', err);
    return { ok: false, error: err.message || String(err) };
  }
}

async function getPendingOrder(userId) {
  try {
    const url = buildReservationApiUrl({ action: 'getPendingOrder', userId });
    const response = await fetch(url);
    return await response.json();
  } catch (err) {
    console.error('getPendingOrder error:', err);
    return { ok: false, error: err.message || String(err), found: false };
  }
}

async function clearPendingOrder(userId) {
  try {
    const response = await fetch(RESERVATION_SAVE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'clearPendingOrder', userId })
    });
    return await response.json();
  } catch (err) {
    console.error('clearPendingOrder error:', err);
    return { ok: false, error: err.message || String(err) };
  }
}

async function listPendingOrders() {
  try {
    const url = buildReservationApiUrl({ action: 'listPendingOrders' });
    const response = await fetch(url);
    const json = await response.json();
    return json;
  } catch (err) {
    console.error('listPendingOrders error:', err);
    return { ok: false, error: err.message || String(err), rows: [] };
  }
}

async function markReminderSent(userId) {
  try {
    const response = await fetch(RESERVATION_SAVE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'markReminderSent', userId })
    });
    return await response.json();
  } catch (err) {
    console.error('markReminderSent error:', err);
    return { ok: false, error: err.message || String(err) };
  }
}

function buildReminderMessages(session) {
  return [
    withNavQuickReply(
      textMessage(
        `ご予約の途中です🍱\n\n` +
          `現在の進行状況：${session.step || '-'}\n` +
          '「続き」で再開できます。'
      ),
      {
        includeBack: false,
        includeCancel: true,
        appendItems: [quickMessageItem('続き', '続き')]
      }
    )
  ];
}

function restoreSessionIfNeeded(userId) {
  return sessions.get(userId) || null;
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

  const pendingList = await listPendingOrders();
  const rows = Array.isArray(pendingList?.rows)
    ? pendingList.rows
    : Array.isArray(pendingList?.data)
      ? pendingList.data
      : [];

  for (const pending of rows) {
    result.checked += 1;

    try {
      const userId = pending.userId || pending.user_id || '';
      if (!userId) {
        result.skipped += 1;
        result.details.push({ status: 'skipped', reason: 'missing userId' });
        continue;
      }

      const updatedAt = new Date(pending.updatedAt || pending.updated_at || pending.createdAt || pending.created_at || 0);
      const diffMinutes = Math.floor((Date.now() - updatedAt.getTime()) / 60000);

      if (!Number.isFinite(diffMinutes) || diffMinutes < PENDING_REMINDER_MINUTES) {
        result.skipped += 1;
        result.details.push({ userId, status: 'skipped', reason: 'not due' });
        continue;
      }

      const alreadySent = String(pending.reminderSent || pending.reminder_sent || '') === 'true';
      if (alreadySent) {
        result.skipped += 1;
        result.details.push({ userId, status: 'skipped', reason: 'already sent' });
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
        userId: pending?.userId || pending?.user_id || '',
        status: 'failed',
        error: err.message || String(err)
      });
    }
  }

  return result;
}

async function replyMessage(replyToken, messages) {
  const response = await fetch('https://api.line.me/v2/bot/message/reply', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${CHANNEL_ACCESS_TOKEN}`
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
      Authorization: `Bearer ${CHANNEL_ACCESS_TOKEN}`
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

function verifySignature(body, signature, secret) {
  if (!secret) return false;
  const hash = crypto
    .createHmac('sha256', secret)
    .update(body)
    .digest('base64');
  return hash === signature;
}

function jstDateTimeToUtcDate(dateText, timeText) {
  const [year, month, day] = String(dateText).split('-').map(Number);
  const [hour, minute] = String(timeText).split(':').map(Number);
  return new Date(Date.UTC(year, month - 1, day, hour - 9, minute, 0));
}

function isStartReservationText(text) {
  const t = normalizeIncomingText(text);
  if (!t) return false;

  if (
    [
      '予約',
      '予約する',
      '弁当予約',
      'ランチ予約',
      'テイクアウト予約'
    ].includes(t)
  ) {
    return true;
  }

  if (t.includes('ご予約に進みます')) {
    return true;
  }

  return false;
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

function isBackText(text) {
  const t = normalizeIncomingText(text);
  return ['一つ前に戻る', '前に戻る', '戻る'].includes(t);
}

function isCancelText(text) {
  const t = normalizeIncomingText(text);
  return ['キャンセルする', 'キャンセル', '予約キャンセル', '中止'].includes(t);
}

function isReservationViewText(text) {
  const t = normalizeIncomingText(text);
  return ['予約内容確認', '予約確認', '内容確認', '予約を確認'].includes(t);
}

function isReservationChangeText(text) {
  const t = normalizeIncomingText(text);
  return ['予約変更', '変更', '予約を変更', '予約の変更'].includes(t);
}

function isNotifyIdText(text) {
  const t = normalizeIncomingText(text);
  return [
    '通知先ID',
    '通知先id',
    '通知ID',
    '通知id',
    'グループID',
    'グループid',
    'groupid',
    'GROUPID'
  ].includes(t);
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

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`APP_VERSION=${APP_VERSION}`);
});
