const express = require('express');
const crypto = require('crypto');
const path = require('path');

const app = express();

const PORT = process.env.PORT || 10000;
const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET || '';
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const RESERVATION_SAVE_URL = process.env.RESERVATION_SAVE_URL || '';
const STORE_NOTIFY_LINE_ID = process.env.STORE_NOTIFY_LINE_ID || '';
const LIFF_ID = process.env.LIFF_ID || '';

const APP_VERSION = '2026-03-27-liiffix-17';

const STORE_NAME = 'かむらど';
const STORE_CODE = 'KMR';
const TIME_ZONE = 'Asia/Tokyo';
const BOOKABLE_DATE_COUNT = 10;

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
    name: '烏龍茶',
    price: 200,
    description: '食事と相性のいい定番ドリンク',
    imageUrl:
      'https://komradefoods1025-geskw.wordpress.com/wp-content/uploads/2026/03/e7838fe9be8de88cb6.webp'
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

  // ここを最初に通すことで、
  // リッチメニューが message でも postback でも拾えるようにする
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

    if (session.step === 'waiting_date' && isYmdDate(text)) {
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
  await beginReservationFlow(replyToken, userId);
  return;
}

    if (data.action === 'begin_change') {
      await beginReservationChangeFlow(replyToken, userId);
      return;
    }

    if (data.action === 'open_name_input') {
      return;
    }

    if (data.action === 'open_phone_input') {
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
        textMessage(`変更後のメニューを選んでください🍱
いまのメニュー内容は一度リセットされます。`),
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

      if (!drink) {
        await savePendingSession(userId, session);
        await replyMessage(replyToken, [
          textMessage('ドリンクが見つかりませんでした。'),
          ...buildMenuStepMessages(session)
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
          buildQtyMessage(getCurrentSelectionLabel(session.currentSelection), 'food')
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

      transitionSession(session, 'waiting_name');
      await savePendingSession(userId, session);

      await replyMessage(replyToken, [
        buildCartSummaryMessage(session),
        buildNameInputMessage()
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
  if (!STORE_NOTIFY_LINE_ID) return;

  const title =
    reservation.status === 'キャンセル'
      ? '【店舗通知：予約キャンセル】'
      : reservation.status === '変更済み'
        ? '【店舗通知：予約変更】'
        : '【店舗通知：新規ランチ予約】';

  const response = await fetch('https://api.line.me/v2/bot/message/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({
      to: STORE_NOTIFY_LINE_ID,
      messages: [
        textMessage(
          `${title}\n\n` +
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
async function startLineLoading(userId, loadingSeconds = 5) {
  const seconds = Math.max(5, Math.min(60, Number(loadingSeconds) || 5));

  if (!userId || !CHANNEL_ACCESS_TOKEN) return;

  try {
    const response = await fetch('https://api.line.me/v2/bot/chat/loading/start', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}`
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
async function beginReservationFlow(replyToken, userId) {
  clearSession(userId);
  await clearPendingSession(userId);

  const session = getSession(userId);
  const bookingConfig = await fetchBookingConfig();

  const rawAvailableDates =
    bookingConfig.ok && Array.isArray(bookingConfig.dates)
      ? bookingConfig.dates
          .map((item) => normalizeYmdDate(item?.date))
          .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
      : [];

  const availableDates = buildEffectiveAvailableDates(rawAvailableDates);

  if (!bookingConfig.ok || !availableDates.length) {
    await replyMessage(replyToken, [
      textMessage('現在ご案内できる営業日がありません。時間をおいてお試しください。')
    ]);
    return;
  }

  session.availableDateOptions = Array.isArray(bookingConfig.dates)
    ? bookingConfig.dates
    : [];
  session.availableDates = availableDates;
  session.history = [];
  session.step = 'waiting_date';

  await savePendingSession(userId, session);

  await replyMessage(replyToken, [createDateSelectMessage()]);
}
function createReservationStartMessage() {
  if (!LIFF_ID) {
    return withNavQuickReply(
      textMessage(
        `${STORE_NAME}のお弁当予約へようこそ🍱\n` +
        `受取日と受取時間を選んでください。\n\n` +
        `受取希望日を YYYY-MM-DD 形式で送ってください。\n` +
        `例：2026-03-26`
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
        '例：2026-03-26'
      ),
      { includeBack: true, includeCancel: true }
    );
  }

  return withNavQuickReply(
    {
      type: 'text',
      text: '受取希望日をカレンダーから選んでください📅',
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

async function handleSelectedDate(replyToken, userId, session, selectedDate) {
  const normalizedSelectedDate = normalizeYmdDate(selectedDate);
  const availableDates = Array.isArray(session.availableDates)
    ? session.availableDates
        .map((date) => normalizeYmdDate(date))
        .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
    : [];

  session.availableDates = availableDates;

  if (!normalizedSelectedDate || !availableDates.includes(normalizedSelectedDate)) {
    await savePendingSession(userId, session);
    await replyMessage(replyToken, [
      textMessage('その日は受付対象外です。営業日から選び直してください。'),
      createDateSelectMessage()
    ]);
    return;
  }

  const availableTimes = getAvailablePickupTimesForDate(normalizedSelectedDate);

  if (!availableTimes.length) {
    await savePendingSession(userId, session);
    await replyMessage(replyToken, [
      textMessage('その日の受付可能時間は終了しました。別日を選んでください。'),
      createDateSelectMessage()
    ]);
    return;
  }

  const nextDailyMenu = await fetchDailyMenuConfig(normalizedSelectedDate);

  if (session.flowType === 'change') {
    transitionSession(session, 'change_waiting_time', {
      date: normalizedSelectedDate,
      dailyMenu: nextDailyMenu
    });
    await savePendingSession(userId, session);

    await replyMessage(replyToken, [
      textMessage(`変更後の受取日：${formatDateWithWeekday(normalizedSelectedDate)}`),
      buildTimeMessage(normalizedSelectedDate)
    ]);
    return;
  }

  transitionSession(session, 'waiting_time', {
    date: normalizedSelectedDate,
    dailyMenu: nextDailyMenu
  });
  await savePendingSession(userId, session);

  await replyMessage(replyToken, [
    textMessage(`受取日：${formatDateWithWeekday(normalizedSelectedDate)}`),
    buildTimeMessage(normalizedSelectedDate)
  ]);
}

async function handleSelectedDateTime(replyToken, userId, session, selectedDate, selectedTime) {
  const normalizedSelectedDate = normalizeYmdDate(selectedDate);
  const normalizedSelectedTime = String(selectedTime || '').trim();

  console.log(`[LIFF DATETIME RECEIVED ${APP_VERSION}]`, {
    selectedDate: normalizedSelectedDate,
    selectedTime: normalizedSelectedTime
  });

  const bookingConfig = await fetchBookingConfig();

  const rawAvailableDates =
    bookingConfig.ok && Array.isArray(bookingConfig.dates)
      ? bookingConfig.dates
          .map((item) => normalizeYmdDate(item?.date))
          .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
      : [];

  const availableDates = buildEffectiveAvailableDates(rawAvailableDates);

  session.availableDateOptions = Array.isArray(bookingConfig.dates)
    ? bookingConfig.dates
    : [];
  session.availableDates = availableDates;

  if (!normalizedSelectedDate || !availableDates.includes(normalizedSelectedDate)) {
    console.log(`[LIFF DATETIME REJECTED DATE ${APP_VERSION}]`, normalizedSelectedDate);
    await savePendingSession(userId, session);
    await replyMessage(replyToken, [
      textMessage('その日は受付対象外です。営業日から選び直してください。'),
      createDateSelectMessage()
    ]);
    return;
  }

  const availableTimes = getAvailablePickupTimesForDate(normalizedSelectedDate);

  if (!availableTimes.includes(normalizedSelectedTime)) {
    console.log(`[LIFF DATETIME REJECTED TIME ${APP_VERSION}]`, normalizedSelectedTime);
    await savePendingSession(userId, session);
    await replyMessage(replyToken, [
      textMessage(
        `受取時間が受付対象外です。\n本日は現在時刻の${SAME_DAY_LEAD_MINUTES}分後以降からご予約いただけます。\nもう一度カレンダーから選んでください。`
      ),
      createDateSelectMessage()
    ]);
    return;
  }

  const nextDailyMenu = await fetchDailyMenuConfig(normalizedSelectedDate);

  if (session.flowType === 'change') {
    transitionSession(session, 'change_menu', {
      date: normalizedSelectedDate,
      time: normalizedSelectedTime,
      dailyMenu: nextDailyMenu
    });
    await savePendingSession(userId, session);

    console.log(`[LIFF DATETIME ACCEPTED ${APP_VERSION}]`, {
      date: session.date,
      time: session.time,
      step: session.step,
      flowType: session.flowType
    });

    await replyMessage(replyToken, [
      textMessage(
        `変更後の受取日：${formatDateWithWeekday(normalizedSelectedDate)}\n変更後の受取時間：${normalizedSelectedTime}`
      ),
      buildChangeCurrentSummaryMessage(session),
      buildChangeMenuMessage(session)
    ]);
    return;
  }

  transitionSession(session, 'waiting_menu', {
    date: normalizedSelectedDate,
    time: normalizedSelectedTime,
    dailyMenu: nextDailyMenu
  });
  await savePendingSession(userId, session);

  console.log(`[LIFF DATETIME ACCEPTED ${APP_VERSION}]`, {
    date: session.date,
    time: session.time,
    step: session.step
  });

  await replyMessage(replyToken, [
    textMessage(
      `受取日：${formatDateWithWeekday(normalizedSelectedDate)}\n受取時間：${normalizedSelectedTime}`
    ),
    ...buildMenuStepMessages(session)
  ]);
}

function buildTimeMessage(selectedDate = '') {
  const availableTimes = getAvailablePickupTimesForDate(selectedDate);

  if (!availableTimes.length) {
    return withNavQuickReply(
      textMessage('この日は選べる受取時間がありません。別日を選んでください🙏'),
      { includeBack: true, includeCancel: true }
    );
  }

  return withNavQuickReply(
    {
      type: 'text',
      text:
        selectedDate && selectedDate === getNowJstDateLabel()
          ? `受取時間を選んでください⏰\n※本日は現在時刻の${SAME_DAY_LEAD_MINUTES}分後以降から選べます`
          : '受取時間を選んでください⏰',
      quickReply: {
        items: availableTimes.map((time) =>
          quickPostbackItem(
            time,
            `action=time&value=${encodeURIComponent(time)}`,
            time
          )
        )
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function getVisibleBentoBubbles(session) {
  const bubbles = [];

  if (session?.dailyMenu?.name) {
    bubbles.push(buildMenuBubble(DAILY_MENU_KEY, session.dailyMenu));
  }

  bubbles.push(
    buildMenuBubble('karaage', MENUS.karaage),
    buildMenuBubble('shogayaki', MENUS.shogayaki),
    buildMenuBubble('chicken_nanban', MENUS.chicken_nanban),
    buildMenuBubble(EXTRA_KARAAGE_KEY, EXTRA_MENUS[EXTRA_KARAAGE_KEY])
  );

  return bubbles;
}

function buildMenuStepMessages(session) {
  const messages = [];
  let intro = 'ご希望の商品をお選びください🍱';

  if (session.items.length) {
    intro +=
      `

現在のご注文
${formatOrderLines(session.items)}` +
      `
合計個数：${getCartTotalQty(session.items)}個` +
      `
注文合計：¥${Number(getCartTotalAmount(session.items)).toLocaleString('ja-JP')}`;
  }

  messages.push(
    withNavQuickReply(textMessage(intro), {
      includeBack: true,
      includeCancel: true
    })
  );
  messages.push(buildMenuFlexMessage(session));
  return messages;
}

function buildMenuFlexMessage(session) {
  return {
    type: 'flex',
    altText: 'お弁当メニュー',
    contents: {
      type: 'carousel',
      contents: getVisibleBentoBubbles(session)
    }
  };
}

function buildDrinkFlexMessage() {
  return {
    type: 'flex',
    altText: 'ドリンクメニュー',
    contents: {
      type: 'carousel',
      contents: DRINK_OPTIONS.map((drink) => buildDrinkBubble(drink))
    }
  };
}

function buildMenuBubble(itemKey, menu) {
  const buttonLabel = itemKey === EXTRA_KARAAGE_KEY ? '追加する' : 'この商品を選ぶ';
  const displayText =
    itemKey === EXTRA_KARAAGE_KEY ? `${menu.name}を追加する` : `${menu.name}を選ぶ`;

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
      contents: bodyContents
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

function buildDrinkBubble(drink) {
  return {
    type: 'bubble',
    size: 'kilo',
    hero: {
      type: 'image',
      url: drink.imageUrl,
      size: 'full',
      aspectRatio: '1:1',
      aspectMode: 'cover'
    },
    body: {
      type: 'box',
      layout: 'vertical',
      spacing: 'sm',
      contents: [
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
            label: 'このドリンクを追加',
            data: `action=drink&item=${drink.key}`,
            displayText: `${drink.name}を追加する`
          }
        }
      ]
    }
  };
}

function getRiceGuideText(menuName) {
  switch (menuName) {
    case 'からあげ弁当':
      return (
        'からあげ弁当ですね！\n' +
        'ジューシーから揚げが4個入りのボリューム満点のお弁当です😋\n' +
        'ご飯の量を選んでください🍚'
      );

    case '生姜焼き弁当':
      return (
        '生姜焼き弁当ですね！\n' +
        'かむらど特製の生姜焼き弁当を是非お試し下さい😋\n' +
        'ご飯の量を選んでください🍚'
      );

    case 'チキン南蛮弁当':
      return (
        'チキン南蛮弁当ですね！\n' +
        'かむらど特製タルタルソースが絶品のお弁当です😋\n' +
        'ご飯の量を選んでください🍚'
      );

    default:
      return `${menuName}ですね🍱\nご飯の量を選んでください🍚`;
  }
}

function buildLargeRiceMessage(menuName) {
  return withNavQuickReply(
    {
      type: 'text',
      text: getRiceGuideText(menuName),
      quickReply: {
        items: [
          quickPostbackItem('小盛り', 'action=rice_size&value=small', 'ご飯は小盛り'),
          quickPostbackItem('普通', 'action=rice_size&value=normal', 'ご飯は普通'),
          quickPostbackItem('大盛り', 'action=rice_size&value=large', 'ご飯は大盛り')
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
    {
      type: 'text',
      text: 'ご連絡先の電話番号を入力してください📞\n例：09012345678',
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
function buildConfirmMessage(session) {
  const items = Array.isArray(session?.items) ? session.items : [];
  const orderLines = items.length ? formatOrderLines(items) : '（まだ商品が入っていません）';
  const totalQty = getCartTotalQty(items);
  const totalAmount = getCartTotalAmount(items);

  return withNavQuickReply(
    {
      type: 'text',
      text:
        `こちらの内容で予約を確定しますか？✨\n\n` +
        `受取日：${session?.date ? formatDateWithWeekday(session.date) : '-'}\n` +
        `受取時間：${session?.time || '-'}\n` +
        `ご注文内容：\n${orderLines}\n` +
        `合計個数：${totalQty}個\n` +
        `注文合計：¥${Number(totalAmount).toLocaleString('ja-JP')}\n` +
        `お名前：${session?.name || '-'}様\n` +
        `電話番号：${session?.phone || '-'}`,
      quickReply: {
        items: [
          quickPostbackItem('この内容で確定', 'action=confirm', 'この内容で確定')
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}
function buildConfirmMessage(session) {
  const items = Array.isArray(session?.items) ? session.items : [];
  const orderLines = items.length ? formatOrderLines(items) : '（まだ商品が入っていません）';
  const totalQty = getCartTotalQty(items);
  const totalAmount = getCartTotalAmount(items);

  return withNavQuickReply(
    {
      type: 'text',
      text:
        `こちらの内容で予約を確定しますか？\n\n` +
        `受取日：${session?.date ? formatDateWithWeekday(session.date) : '-'}\n` +
        `受取時間：${session?.time || '-'}\n` +
        `ご注文内容：\n${orderLines}\n` +
        `合計個数：${totalQty}個\n` +
        `注文合計：¥${Number(totalAmount).toLocaleString('ja-JP')}\n` +
        `お名前：${session?.name || '-'}様\n` +
        `電話番号：${session?.phone || '-'}`,
      quickReply: {
        items: [
          quickPostbackItem('この内容で確定', 'action=confirm', 'この内容で確定')
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}
function buildQtyMessage(itemName, itemType = 'food') {
  const icon = itemType === 'drink' ? '🥤' : '🍱';
  return withNavQuickReply(
    {
      type: 'text',
      text: `${itemName} の個数を選んでください${icon}`,
      quickReply: {
        items: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map((n) =>
          quickPostbackItem(`${n}個`, `action=qty&value=${n}`, `${n}個`)
        )
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function parseQtyText(text) {
  const normalized = String(text || '')
    .normalize('NFKC')
    .replace(/\s+/g, '')
    .trim();

  if (!normalized) return 0;

  const match = normalized.match(/^(\d{1,2})(?:個)?$/);
  if (!match) return 0;

  const qty = Number(match[1] || 0);
  if (!Number.isInteger(qty)) return 0;
  if (qty < 1 || qty > 10) return 0;

  return qty;
}

async function handleQtySelection(replyToken, userId, session, qty) {
  const safeQty = Number(qty || 0);

  if (!safeQty || !session?.currentSelection) {
    await savePendingSession(userId, session);
    await replyMessage(replyToken, [
      textMessage('個数をもう一度選んでください。'),
      buildQtyMessage(
        getCurrentSelectionLabel(session?.currentSelection),
        session?.currentSelection?.itemType || 'food'
      )
    ]);
    return;
  }

  const selection = { ...session.currentSelection };

  addItemToCart(session, {
    itemType: selection.itemType || 'food',
    menuKey: selection.menuKey,
    menuName: selection.menuName,
    price: Number(selection.price || 0),
    riceSize: selection.riceSize || '',
    qty: safeQty
  });

  const addedMessages = [
    textMessage(`${getCurrentSelectionLabel(selection)} を ${safeQty}個 追加しました！`)
  ];

  if (selection.drinkKey && selection.drinkName) {
    addItemToCart(session, {
      itemType: 'drink',
      menuKey: selection.drinkKey,
      menuName: selection.drinkName,
      price: Number(selection.drinkPrice || 0),
      riceSize: '',
      qty: safeQty
    });

    addedMessages.push(
      textMessage(`${selection.drinkName} を ${safeQty}個 追加しました！`)
    );
  }

  if (session.flowType === 'change') {
    transitionSession(session, 'change_menu', { currentSelection: null });
    await savePendingSession(userId, session);

    await replyMessage(replyToken, [
      ...addedMessages,
      buildChangeCurrentSummaryMessage(session),
      buildChangeMenuMessage(session)
    ]);
    return;
  }

  transitionSession(session, 'menu_or_review', { currentSelection: null });
  await savePendingSession(userId, session);

  await replyMessage(replyToken, [
    ...addedMessages,
    buildCartSummaryMessage(session),
    buildCartActionMessage()
  ]);
}

function isQtyText(text) {
  return parseQtyText(text) > 0;
}

function buildChangeCurrentSummaryMessage(session) {
  return textMessage(
    `変更後の予約内容です💁‍♀️

` +
      `受付番号：${session.editingReservationNo || '-'}
` +
      `受取日：${session.date ? formatDateWithWeekday(session.date) : '-'}
` +
      `受取時間：${session.time || '-'}
` +
      `ご注文内容：
${formatOrderLines(session.items || [])}
` +
      `合計個数：${getCartTotalQty(session.items || [])}個
` +
      `注文合計：¥${Number(getCartTotalAmount(session.items || [])).toLocaleString('ja-JP')}
` +
      `お名前：${session.name || '-'}様
` +
      `電話番号：${session.phone || '-'}
` +
      `ステータス：変更受付中`
  );
}

function safeChangeMenuText(value, fallback = '未設定') {
  const text = String(value == null ? '' : value).trim();
  return text || fallback;
}

function formatPhoneForDisplay(phone) {
  const raw = String(phone == null ? '' : phone).replace(/[^0-9]/g, '');

  if (raw.length === 11) {
    return `${raw.slice(0, 3)}-${raw.slice(3, 7)}-${raw.slice(7)}`;
  }

  if (raw.length === 10) {
    if (raw.startsWith('03') || raw.startsWith('06')) {
      return `${raw.slice(0, 2)}-${raw.slice(2, 6)}-${raw.slice(6)}`;
    }
    return `${raw.slice(0, 3)}-${raw.slice(3, 6)}-${raw.slice(6)}`;
  }

  return safeChangeMenuText(phone, '未設定');
}

function buildChangeItemsText(items) {
  if (!Array.isArray(items) || !items.length) return '未設定';

  return (
    `${formatOrderLines(items)}
` +
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
              label: '予約自体をキャンセル',
              data: `action=${CHANGE_CANCEL_REQUEST_ACTION}`,
              displayText: '予約自体をキャンセル'
            }
          },
          {
            type: 'button',
            style: 'secondary',
            height: 'sm',
            action: {
              type: 'postback',
              label: '一つ前に戻る',
              data: `action=${BACK_ACTION}`,
              displayText: BACK_DISPLAY_TEXT
            }
          },
          {
            type: 'button',
            style: 'secondary',
            height: 'sm',
            action: {
              type: 'postback',
              label: '変更をやめる',
              data: `action=${CANCEL_ACTION}`,
              displayText: CANCEL_DISPLAY_TEXT
            }
          }
        ]
      }
    }
  };
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
      text: `変更後の電話番号をご入力してください🤙\n例：09012345678`,
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

function buildCartSummaryMessage(session) {
  const items = Array.isArray(session?.items) ? session.items : [];
  const orderLines = items.length ? formatOrderLines(items) : '（まだ商品が入っていません）';
  const totalQty = getCartTotalQty(items);
  const totalAmount = getCartTotalAmount(items);

  return textMessage(
    `現在のご注文内容です🧾\n\n` +
      `受取日：${session?.date ? formatDateWithWeekday(session.date) : '-'}\n` +
      `受取時間：${session?.time || '-'}\n` +
      `ご注文内容：\n${orderLines}\n` +
      `合計個数：${totalQty}個\n` +
      `注文合計：¥${Number(totalAmount).toLocaleString('ja-JP')}`
  );
}

function buildCartActionMessage() {
  return withNavQuickReply(
    {
      type: 'text',
      text: 'ご注文を続けるか、注文内容の確認へ進んでください😊',
      quickReply: {
        items: [
          quickPostbackItem('商品を追加する', 'action=add_more', '商品を追加する'),
          quickPostbackItem('注文内容を確認する', 'action=review_order', '注文内容を確認する')
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function buildCartSummaryMessage(session) {
  const items = Array.isArray(session?.items) ? session.items : [];
  const orderLines = items.length ? formatOrderLines(items) : '（まだ商品が入っていません）';
  const totalQty = getCartTotalQty(items);
  const totalAmount = getCartTotalAmount(items);

  return textMessage(
    `現在のご注文内容です🧾\n\n` +
      `受取日：${session?.date ? formatDateWithWeekday(session.date) : '-'}\n` +
      `受取時間：${session?.time || '-'}\n` +
      `ご注文内容：\n${orderLines}\n` +
      `合計個数：${totalQty}個\n` +
      `注文合計：¥${Number(totalAmount).toLocaleString('ja-JP')}`
  );
}

function buildCartSummaryMessage(session) {
  const items = Array.isArray(session?.items) ? session.items : [];
  const orderLines = items.length ? formatOrderLines(items) : '（まだ商品が入っていません）';
  const totalQty = getCartTotalQty(items);
  const totalAmount = getCartTotalAmount(items);

  return textMessage(
    `現在のご注文内容です🧾\n\n` +
      `受取日：${session?.date ? formatDateWithWeekday(session.date) : '-'}\n` +
      `受取時間：${session?.time || '-'}\n` +
      `ご注文内容：\n${orderLines}\n` +
      `合計個数：${totalQty}個\n` +
      `注文合計：¥${Number(totalAmount).toLocaleString('ja-JP')}`
  );
}

function buildCartActionMessage() {
  return withNavQuickReply(
    {
      type: 'text',
      text: 'ご注文を続けるか、注文内容の確認へ進んでください😊',
      quickReply: {
        items: [
          quickPostbackItem('商品を追加する', 'action=add_more', '商品を追加する'),
          quickPostbackItem('注文内容を確認する', 'action=review_order', '注文内容を確認する')
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function buildCartSummaryMessage(session) {
  const items = Array.isArray(session?.items) ? session.items : [];
  const orderLines = items.length ? formatOrderLines(items) : '（まだ商品が入っていません）';
  const totalQty = getCartTotalQty(items);
  const totalAmount = getCartTotalAmount(items);

  return textMessage(
    `現在のご注文内容です🧾\n\n` +
      `受取日：${session?.date ? formatDateWithWeekday(session.date) : '-'}\n` +
      `受取時間：${session?.time || '-'}\n` +
      `ご注文内容：\n${orderLines}\n` +
      `合計個数：${totalQty}個\n` +
      `注文合計：¥${Number(totalAmount).toLocaleString('ja-JP')}`
  );
}

function buildCartActionMessage() {
  return withNavQuickReply(
    {
      type: 'text',
      text: 'ご注文を続けるか、注文内容の確認へ進んでください😊',
      quickReply: {
        items: [
          quickPostbackItem('商品を追加する', 'action=add_more', '商品を追加する'),
          quickPostbackItem('注文内容を確認する', 'action=review_order', '注文内容を確認する')
        ]
      }
    },
    { includeBack: true, includeCancel: true }
  );
}

function buildReservationChangedMessage(reservation) {
  return textMessage(
    `予約変更を受け付けました✨\n\n` +
      `受付番号：${reservation.reservationNo}\n` +
      `受取日：${formatDateWithWeekday(reservation.date)}\n` +
      `受取時間：${reservation.time}\n` +
      `ご注文内容：\n${formatOrderLines(reservation.items)}\n` +
      `合計個数：${reservation.totalQty}個\n` +
      `注文合計：¥${Number(reservation.total).toLocaleString('ja-JP')}\n` +
      `お名前：${reservation.name}様\n` +
      `電話番号：${reservation.phone}\n` +
      `ステータス：${reservation.status}`
  );
}
function startGuideMessage() {
  return {
    type: 'text',
    text: `${STORE_NAME}のお弁当予約フォームです！\n「予約を始める」を押していただければ開始できます😊`,
    quickReply: {
      items: [quickPostbackItem('予約を始める', 'action=reserve_start', '予約を始める')]
    }
  };
}

function buildResumeMessages(session) {
  switch (session.step) {
    case 'waiting_date':
      return [
        textMessage('ご予約の続きから再開できます。'),
        createDateSelectMessage()
      ];

    case 'waiting_time':
      return [
        textMessage('ご予約の続きをご案内します。'),
        session.date
          ? textMessage(`受取日：${formatDateWithWeekday(session.date)}`)
          : textMessage(''),
        buildTimeMessage(session.date)
      ];

    case 'waiting_menu':
      return [textMessage('ご予約の続きをご案内します。'), ...buildMenuStepMessages(session)];

    case 'waiting_rice_size':
      return [
        textMessage('ご予約の続きをご案内します。'),
        buildLargeRiceMessage(session.currentSelection?.menuName || '商品')
      ];

    case 'waiting_drink_confirm':
      return [
        textMessage('ご予約の続きをご案内します。'),
        buildDrinkConfirmMessage(session.currentSelection?.menuName || '商品')
      ];

    case 'waiting_drink_menu':
      return [
        textMessage('ご予約の続きをご案内します。'),
        withNavQuickReply(textMessage('付けるドリンクを選んでください🥤'), { includeBack: true, includeCancel: true }),
        buildDrinkFlexMessage()
      ];

    case 'waiting_qty':
      return [
        textMessage('ご予約の続きをご案内します。'),
        textMessage(`ご注文商品：${getCurrentSelectionLabel(session.currentSelection)}`),
        buildQtyMessage(
          getCurrentSelectionLabel(session.currentSelection),
          session.currentSelection?.itemType || 'food'
        )
      ];

    case 'menu_or_review':
      return [
        textMessage('ご予約の続きをご案内します。'),
        buildCartSummaryMessage(session),
        buildCartActionMessage()
      ];

    case 'waiting_name':
      return [buildCartSummaryMessage(session), buildNameInputMessage()];

    case 'waiting_phone':
      return [
        textMessage(`ご予約名：${session.name || ''}`),
        buildPhoneInputMessage()
      ];

    case 'confirm':
      return [buildConfirmMessage(session)];

    case 'change_menu':
      return [buildChangeCurrentSummaryMessage(session), buildChangeMenuMessage(session)];

    case 'change_waiting_date':
      return [textMessage('変更後の受取日を選んでください📆'), createDateSelectMessage()];

    case 'change_waiting_time':
      return [
        textMessage(`変更後の受取時間を選んでください⏰\n現在の受取日📆：${formatDateWithWeekday(session.date)}`),
        buildTimeMessage(session.date)
      ];

    case 'change_waiting_name':
      return [buildChangeNameInputMessage()];

    case 'change_waiting_phone':
      return [buildChangePhoneInputMessage()];

    default:
      return [startGuideMessage()];
  }
}

function buildReminderMessages(session) {
  const head = textMessage(
    'ご注文を続けますか🤔？\n5分以上操作がなかったため、続きからご案内します。'
  );

  switch (session.step) {
    case 'waiting_date':
      return [head, createDateSelectMessage()];

    case 'waiting_time':
      return [head, buildTimeMessage(session.date)];

    case 'waiting_menu':
      return [head, ...buildMenuStepMessages(session)];

    case 'waiting_rice_size':
      return [head, buildLargeRiceMessage(session.currentSelection?.menuName || '商品')];

    case 'waiting_drink_confirm':
      return [head, buildDrinkConfirmMessage(session.currentSelection?.menuName || '商品')];

    case 'waiting_drink_menu':
      return [head, withNavQuickReply(textMessage('付けるドリンクを選んでください🥤'), { includeBack: true, includeCancel: true }), buildDrinkFlexMessage()];

    case 'waiting_qty':
      return [
        head,
        textMessage(`ご注文商品：${getCurrentSelectionLabel(session.currentSelection)}`),
        buildQtyMessage(
          getCurrentSelectionLabel(session.currentSelection),
          session.currentSelection?.itemType || 'food'
        )
      ];

    case 'menu_or_review':
      return [head, buildCartSummaryMessage(session), buildCartActionMessage()];

    case 'waiting_name':
      return [head, buildCartSummaryMessage(session), buildNameInputMessage()];

    case 'waiting_phone':
      return [head, buildPhoneInputMessage()];

    case 'confirm':
      return [head, buildConfirmMessage(session)];

    case 'change_menu':
      return [head, buildChangeCurrentSummaryMessage(session), buildChangeMenuMessage(session)];

    case 'change_waiting_date':
      return [head, textMessage('変更後の受取日を選んでください📆'), createDateSelectMessage()];

    case 'change_waiting_time':
      return [
        head,
        textMessage(`変更後の受取時間を選んでください⏰\n現在の受取日📆：${formatDateWithWeekday(session.date)}`),
        buildTimeMessage(session.date)
      ];

    case 'change_waiting_name':
      return [head, buildChangeNameInputMessage()];

    case 'change_waiting_phone':
      return [head, buildChangePhoneInputMessage()];

    default:
      return [head, startGuideMessage()];
  }
}

function quickPostbackItem(label, data, displayText, options = {}) {
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

function textMessage(text) {
  return { type: 'text', text };
}

function buildNavQuickReplyItems({ includeBack = true, includeCancel = true } = {}) {
  const items = [];

  if (includeBack) {
    items.push(quickPostbackItem('一つ前に戻る', `action=${BACK_ACTION}`, BACK_DISPLAY_TEXT));
  }

  if (includeCancel) {
    items.push(
      quickPostbackItem('キャンセルする', `action=${CANCEL_ACTION}`, CANCEL_DISPLAY_TEXT)
    );
  }

  return items;
}

function withNavQuickReply(message, options = {}) {
  const navItems = buildNavQuickReplyItems(options);
  const currentItems = Array.isArray(message?.quickReply?.items)
    ? message.quickReply.items
    : [];

  return {
    ...message,
    quickReply: {
      items: [...currentItems, ...navItems]
    }
  };
}

async function handleBackAction(replyToken, userId, session) {
  if (!goBackSession(session)) {
    await savePendingSession(userId, session);
    await replyMessage(replyToken, [
      textMessage('これ以上戻れません。'),
      ...buildResumeMessages(session)
    ]);
    return;
  }

  await savePendingSession(userId, session);
  await replyMessage(replyToken, buildResumeMessages(session));
}

async function handleCancelAction(replyToken, userId) {
  clearSession(userId);
  await clearPendingSession(userId);
  await replyMessage(replyToken, [
    textMessage('予約をキャンセルしました。'),
    startGuideMessage()
  ]);
}

function resolveMenuByKey(session, key) {
  if (key === DAILY_MENU_KEY) {
    return session?.dailyMenu?.name ? session.dailyMenu : null;
  }

  if (EXTRA_MENUS[key]) {
    return EXTRA_MENUS[key];
  }

  return MENUS[key] || null;
}

function resolveDrinkByKey(key) {
  return DRINK_OPTIONS.find((drink) => drink.key === key) || null;
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

function createEmptySession() {
  return {
    date: '',
    time: '',
    name: '',
    phone: '',
    items: [],
    currentSelection: null,
    dailyMenu: null,
    availableDates: [],
    availableDateOptions: [],
    history: [],
    flowType: 'new',
    editingReservationNo: '',
    editingStatus: '',
    step: ''
  };
}

function cloneSessionValue(value) {
  return JSON.parse(JSON.stringify(value));
}

function createSessionSnapshot(session) {
  return cloneSessionValue({
    date: session?.date || '',
    time: session?.time || '',
    name: session?.name || '',
    phone: session?.phone || '',
    items: Array.isArray(session?.items) ? session.items : [],
    currentSelection: session?.currentSelection || null,
    dailyMenu: session?.dailyMenu || null,
    availableDates: Array.isArray(session?.availableDates) ? session.availableDates : [],
    availableDateOptions: Array.isArray(session?.availableDateOptions)
      ? session.availableDateOptions
      : [],
    flowType: session?.flowType || 'new',
    editingReservationNo: session?.editingReservationNo || '',
    editingStatus: session?.editingStatus || '',
    step: session?.step || ''
  });
}

function pushSessionHistory(session) {
  if (!session) return;

  if (!Array.isArray(session.history)) {
    session.history = [];
  }

  session.history.push(createSessionSnapshot(session));

  if (session.history.length > 30) {
    session.history.shift();
  }
}

function transitionSession(session, nextStep, patch = {}) {
  pushSessionHistory(session);
  Object.assign(session, patch || {});
  session.step = nextStep;
}

function restoreSessionFromSnapshot(session, snapshot) {
  const history = Array.isArray(session?.history) ? session.history : [];
  Object.assign(session, createEmptySession(), snapshot || {}, { history });
}

function goBackSession(session) {
  if (!session || !Array.isArray(session.history) || !session.history.length) {
    return false;
  }

  const previous = session.history.pop();
  restoreSessionFromSnapshot(session, previous);
  return true;
}

function clearSession(userId) {
  sessions.set(userId, createEmptySession());
}

function canOfferDrinkForSelection(selection) {
  if (!selection) return false;
  if (selection.itemType !== 'food') return false;
  if (!selection.menuKey) return false;
  return selection.menuKey !== EXTRA_KARAAGE_KEY;
}

function restoreSessionFromPending(pending) {
  const items = safeJsonParse(pending.itemsJson, []);
  const currentSelection = safeJsonParse(pending.currentSelectionJson, null);
  const availableDates = safeJsonParse(pending.availableDatesJson, []);
  const availableDateOptions = safeJsonParse(pending.availableDateOptionsJson, []);
  const dailyMenuRaw = safeJsonParse(pending.dailyMenuJson, null);
  const historyRaw = safeJsonParse(pending.historyJson, []);

  return {
    date: normalizeYmdDate(pending.date || ''),
    time: pending.time || '',
    name: pending.name || '',
    phone: pending.phone || '',
    items: Array.isArray(items) ? items : [],
    currentSelection: currentSelection || null,
    dailyMenu: dailyMenuRaw?.name ? withMenuDefaults(dailyMenuRaw) : null,
    availableDates: Array.isArray(availableDates)
      ? availableDates
          .map((date) => normalizeYmdDate(date))
          .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
      : [],
    availableDateOptions: Array.isArray(availableDateOptions) ? availableDateOptions : [],
    history: Array.isArray(historyRaw) ? historyRaw : [],
    flowType: pending.flowType || 'new',
    editingReservationNo: pending.editingReservationNo || '',
    editingStatus: pending.editingStatus || '',
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
  const existing = session.items.find(
    (item) =>
      item.menuKey === newItem.menuKey &&
      (item.riceSize || '') === (newItem.riceSize || '')
  );

  if (existing) {
    existing.qty += newItem.qty;
    existing.total = existing.qty * existing.price;
    return;
  }

  session.items.push({
    itemType: newItem.itemType || 'food',
    menuKey: newItem.menuKey,
    menuName: newItem.menuName,
    price: Number(newItem.price || 0),
    riceSize: newItem.riceSize || '',
    qty: Number(newItem.qty || 0),
    total: Number(newItem.price || 0) * Number(newItem.qty || 0)
  });
}

function getLargeRiceQty(items) {
  return (items || []).reduce((sum, item) => {
    if ((item.riceSize || '') === '大盛り') {
      return sum + Number(item.qty || 0);
    }
    return sum;
  }, 0);
}

function hasDrinkItems(items) {
  return (items || []).some(
    (item) => item.itemType === 'drink' || String(item.menuKey || '').startsWith(DRINK_KEY_PREFIX)
  );
}

function formatFoodLines(items) {
  const foods = (items || []).filter(
    (item) => !(item.itemType === 'drink' || String(item.menuKey || '').startsWith(DRINK_KEY_PREFIX))
  );
  return formatOrderLines(foods);
}

function formatDrinkLines(items) {
  const drinks = (items || []).filter(
    (item) => item.itemType === 'drink' || String(item.menuKey || '').startsWith(DRINK_KEY_PREFIX)
  );
  return formatOrderLines(drinks);
}

function formatOrderLines(items) {
  if (!items || !items.length) return '・商品が入っていません';

  return items
    .map(
      (item) =>
        `・${getDisplayMenuName(item)} ×${item.qty}個　¥${Number(item.total).toLocaleString('ja-JP')}`
    )
    .join('\n');
}
function normalizeRiceSizeLabel(value) {
  const v = String(value || '').trim().toLowerCase();

  if (v === 'small' || v === '小' || v === '小盛り') return '小盛り';
  if (v === 'large' || v === 'yes' || v === '大' || v === '大盛り') return '大盛り';
  if (v === 'normal' || v === 'no' || v === '普通') return '普通';

  return '';
}
function getDisplayMenuName(item) {
  if (!item) return '商品';

  const riceSize = normalizeRiceSizeLabel(item.riceSize);

  if (riceSize === '小盛り') {
    return `${item.menuName}（ご飯小盛り）`;
  }

  if (riceSize === '大盛り') {
    return `${item.menuName}（ご飯大盛り）`;
  }

  return item.menuName;
}

function getCurrentSelectionLabel(selection) {
  if (!selection) return '商品';
  return getDisplayMenuName(selection);
}

function getCartTotalQty(items) {
  return (items || []).reduce((sum, item) => sum + Number(item.qty || 0), 0);
}

function reservationFromApiRow(row) {
  const items = safeJsonParse(row.itemsJson || '[]', []);
  return {
    reservationNo: row.reservationNo || '',
    userId: row.userId || '',
    date: normalizeYmdDate(row.date || ''),
    time: row.time || '',
    items: Array.isArray(items) ? items : [],
    itemCount: Number(row.itemCount || (Array.isArray(items) ? items.length : 0) || 0),
    totalQty: Number(row.totalQty || getCartTotalQty(items) || 0),
    total: Number(row.total || getCartTotalAmount(items) || 0),
    name: row.name || '',
    phone: row.phone || '',
    status: row.status || '',
    createdAt: row.createdAt || '',
    orderLines: row.orderLines || formatOrderLines(items)
  };
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

    if (!response.ok) return null;

    const json = JSON.parse(text);
    if (!json.ok || !json.found) return null;
    if (json.visible === false) return null;
    if (String(json.isHidden || '').toLowerCase() === 'true') return null;
    if (String(json.hidden || '').toLowerCase() === 'true') return null;

    return withMenuDefaults({
      name: json.menuName || DEFAULT_DAILY_MENU.name,
      price: Number(json.price || DEFAULT_DAILY_MENU.price),
      description: json.description || '',
      imageUrl: json.imageUrl || DEFAULT_DAILY_MENU.imageUrl,
      allowLargeRice: true
    });
  } catch {
    return null;
  }
}

function withMenuDefaults(menu) {
  return {
    name: menu.name || DEFAULT_DAILY_MENU.name,
    price: Number(menu.price || DEFAULT_DAILY_MENU.price),
    description: menu.description || '',
    imageUrl: menu.imageUrl || DEFAULT_DAILY_MENU.imageUrl,
    allowLargeRice:
      typeof menu.allowLargeRice === 'boolean'
        ? menu.allowLargeRice
        : DEFAULT_DAILY_MENU.allowLargeRice
  };
}

async function updateReservationOnSheet(reservation) {
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

async function fetchLatestReservation(userId) {
  try {
    const url = buildReservationApiUrl({
      action: 'getLatestReservation',
      userId
    });

    const response = await fetch(url);
    const text = await response.text();

    if (!response.ok) return { ok: false, found: false, error: text };

    const json = JSON.parse(text);
    if (!json.ok || !json.found) {
      return { ok: true, found: false };
    }

    return {
      ok: true,
      found: true,
      reservation: reservationFromApiRow(json.reservation || json)
    };
  } catch (err) {
    return { ok: false, found: false, error: String(err) };
  }
}

async function cancelReservationOnSheet(reservation) {
  try {
    const items = Array.isArray(reservation.items) ? reservation.items : [];

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
      itemsJson: JSON.stringify(items),
      itemCount: String(reservation.itemCount || 0),
      totalQty: String(reservation.totalQty || 0),
      total: String(reservation.total || 0),
      orderLines: formatOrderLines(items),
      foodLines: formatFoodLines(items),
      drinkLines: formatDrinkLines(items),
      hasDrink: hasDrinkItems(items) ? 'yes' : 'no',
      hasLargeRice: getLargeRiceQty(items) > 0 ? 'yes' : 'no',
      largeRiceQty: String(getLargeRiceQty(items)),
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

function buildReservationCanceledMessage(reservation) {
  return textMessage(
    `ご予約のキャンセルを受け付けました。\n\n` +
      `受付番号：${reservation.reservationNo}\n` +
      `受取日：${reservation.date ? formatDateWithWeekday(reservation.date) : '-'}\n` +
      `受取時間：${reservation.time || '-'}\n` +
      `ご注文内容：\n${formatOrderLines(reservation.items || [])}\n` +
      `お名前：${reservation.name || '-'}様\n` +
      `電話番号：${formatPhoneForDisplay(reservation.phone || '')}\n` +
      `ステータス：${reservation.status || 'キャンセル'}`
  );
}

async function handleReservationCancelConfirm(replyToken, userId, session) {
  if (!session?.editingReservationNo) {
    await replyMessage(replyToken, [
      textMessage('キャンセル対象の予約が見つかりません。もう一度お試しください。')
    ]);
    return;
  }

  const items = Array.isArray(session.items)
    ? session.items.map((item) => ({ ...item }))
    : [];
  const canceledAt = getJstDateTimeLabel();

  const reservation = {
    reservationNo: session.editingReservationNo,
    userId,
    date: session.date,
    time: session.time,
    items,
    itemCount: items.length,
    totalQty: getCartTotalQty(items),
    total: getCartTotalAmount(items),
    name: session.name,
    phone: session.phone,
    status: 'キャンセル',
    canceledAt,
    updatedAt: canceledAt
  };

  const saveResult = await cancelReservationOnSheet(reservation);

  if (!saveResult.ok) {
    await replyMessage(replyToken, [
      textMessage(
        `予約キャンセルの保存でエラーが起きました。\n${saveResult.error || 'unknown error'}`
      )
    ]);
    return;
  }

  notifyStoreByLine({
    reservation,
    createdAt: canceledAt,
    totalQty: reservation.totalQty,
    total: reservation.total
  }).catch((err) => console.error('store line notify error:', err));

  clearSession(userId);
  await clearPendingSession(userId);

  await replyMessage(replyToken, [buildReservationCanceledMessage(reservation)]);
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

  return textMessage(
    `現在のご予約内容です📋\n\n` +
      `受付番号：${reservation?.reservationNo || '-'}\n` +
      `受取日：${reservation?.date ? formatDateWithWeekday(reservation.date) : '-'}\n` +
      `受取時間：${reservation?.time || '-'}\n` +
      `ご注文内容：\n${formatOrderLines(items)}\n` +
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
  const latestResult = await fetchLatestReservation(userId);

  if (!latestResult.ok) {
    await replyMessage(replyToken, [
      textMessage(
        `予約内容の取得でエラーが起きました。\n${latestResult.error || 'unknown error'}`
      )
    ]);
    return;
  }

  if (!latestResult.found) {
    await replyMessage(replyToken, [
      textMessage('変更できるご予約が見つかりませんでした。')
    ]);
    return;
  }

  const reservation = latestResult.reservation;

  // 30分未満なら変更画面に入れない
  if (isReservationChangeLocked(reservation)) {
    await replyMessage(replyToken, [
      buildReservationChangeLockedMessage(reservation),
      buildLatestReservationMessage(reservation)
    ]);
    return;
  }

  const bookingConfig = await fetchBookingConfig();
  const rawAvailableDates =
    bookingConfig.ok && Array.isArray(bookingConfig.dates)
      ? bookingConfig.dates
          .map((item) => normalizeYmdDate(item?.date))
          .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date))
      : [];

  const availableDates = buildEffectiveAvailableDates(rawAvailableDates);

  clearSession(userId);
  const session = getSession(userId);

  Object.assign(session, {
    flowType: 'change',
    editingReservationNo: reservation.reservationNo,
    editingStatus: reservation.status || '受付済み',
    date: reservation.date,
    time: reservation.time,
    name: reservation.name,
    phone: reservation.phone,
    items: Array.isArray(reservation.items)
      ? reservation.items.map((item) => ({ ...item }))
      : [],
    currentSelection: null,
    dailyMenu: await fetchDailyMenuConfig(reservation.date),
    availableDateOptions:
      bookingConfig.ok && Array.isArray(bookingConfig.dates)
        ? bookingConfig.dates
        : [],
    availableDates,
    history: [],
    step: 'change_menu'
  });

  await savePendingSession(userId, session);

  await replyMessage(replyToken, [
    buildLatestReservationMessage(reservation),
    buildChangeCurrentSummaryMessage(session),
    buildChangeMenuMessage(session)
  ]);
}

async function handleReservationChangeConfirm(replyToken, userId, session) {
  if (!session?.editingReservationNo) {
    await replyMessage(replyToken, [
      textMessage('変更対象の予約が見つかりません。もう一度お試しください。')
    ]);
    return;
  }

  // 最終確定前に、最新の予約状態を再取得して再チェック
  const latestResult = await fetchLatestReservation(userId);

  if (!latestResult.ok) {
    await replyMessage(replyToken, [
      textMessage(
        `予約内容の取得でエラーが起きました。\n${latestResult.error || 'unknown error'}`
      )
    ]);
    return;
  }

  if (!latestResult.found) {
    await replyMessage(replyToken, [
      textMessage('変更対象の予約が見つかりませんでした。')
    ]);
    return;
  }

  const latestReservation = latestResult.reservation;

  if (
    latestReservation?.reservationNo &&
    session.editingReservationNo &&
    latestReservation.reservationNo !== session.editingReservationNo
  ) {
    await replyMessage(replyToken, [
      textMessage('変更対象の予約が見つかりません。もう一度ご予約内容をご確認ください。')
    ]);
    return;
  }

  // 変更途中で30分を切った場合もここで止める
  if (isReservationChangeLocked(latestReservation)) {
    await replyMessage(replyToken, [
      buildReservationChangeLockedMessage(latestReservation),
      buildLatestReservationMessage(latestReservation)
    ]);
    return;
  }

  const items = Array.isArray(session.items)
    ? session.items.map((item) => ({ ...item }))
    : [];

  const reservation = {
    reservationNo: session.editingReservationNo,
    userId,
    date: session.date,
    time: session.time,
    items,
    itemCount: items.length,
    totalQty: getCartTotalQty(items),
    total: getCartTotalAmount(items),
    name: session.name,
    phone: session.phone,
    status: '変更済み',
    updatedAt: getJstDateTimeLabel()
  };

  const saveResult = await updateReservationOnSheet(reservation);

  if (!saveResult.ok) {
    await replyMessage(replyToken, [
      textMessage(
        `予約変更の保存でエラーが起きました。\n${saveResult.error || 'unknown error'}`
      )
    ]);
    return;
  }

  notifyStoreByLine({
    reservation,
    createdAt: reservation.updatedAt,
    totalQty: reservation.totalQty,
    total: reservation.total
  }).catch((err) => console.error('store line notify error:', err));

  clearSession(userId);
  await clearPendingSession(userId);

  await replyMessage(replyToken, [buildReservationChangedMessage(reservation)]);
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
      date: normalizeYmdDate(session.date || ''),
      time: session.time || '',
      itemsJson: JSON.stringify(session.items || []),
      currentSelectionJson: JSON.stringify(session.currentSelection || null),
      name: session.name || '',
      phone: session.phone || '',
      availableDatesJson: JSON.stringify(
        (session.availableDates || []).map((date) => normalizeYmdDate(date))
      ),
      availableDateOptionsJson: JSON.stringify(session.availableDateOptions || []),
      historyJson: JSON.stringify(session.history || []),
      flowType: session.flowType || 'new',
      editingReservationNo: session.editingReservationNo || '',
      editingStatus: session.editingStatus || '',
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
    return json.ok
      ? json
      : { ok: false, error: json.error || 'getReminderTargets error', targets: [] };
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

  const title =
    reservation.status === 'キャンセル'
      ? '【店舗通知：予約キャンセル】'
      : reservation.status === '変更済み'
        ? '【店舗通知：予約変更】'
        : '【店舗通知：新規ランチ予約】';

  const response = await fetch('https://api.line.me/v2/bot/message/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({
      to: STORE_NOTIFY_LINE_ID,
      messages: [
        textMessage(
          `${title}\n\n` +
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

function verifySignature(rawBody, signature, secret) {
  if (!secret || !signature) return false;

  const hash = crypto.createHmac('SHA256', secret).update(rawBody).digest('base64');

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
  return String(text || '')
    .normalize('NFKC')
    .replace(/[０-９]/g, (s) => String.fromCharCode(s.charCodeAt(0) - 0xFEE0))
    .replace(/[^\d]/g, '');
}

function isValidPhone(phone) {
  const normalized = normalizePhone(phone);

  if (!/^0\d{9,10}$/.test(normalized)) return false;
  if (/^(\d)\1{9,10}$/.test(normalized)) return false;

  if (/^(070|080|090)\d{8}$/.test(normalized)) return true;
  if (/^050\d{8}$/.test(normalized)) return true;
  if (/^0800\d{7}$/.test(normalized)) return true;
  if (/^(0120|0570|0990)\d{6}$/.test(normalized)) return true;
  if (/^0[1-9]\d{8}$/.test(normalized)) return true;
  if (/^0[1-9]\d{9}$/.test(normalized)) return true;

  return false;
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
    .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date));

  const mergedDateSet = new Set(normalized);
  const todayJst = getNowJstDateLabel(now);

  if (normalized.length > 0 && getAvailablePickupTimesForDate(todayJst, now).length > 0) {
    mergedDateSet.add(todayJst);
  }

  return filterAvailableDatesByPickupTime(Array.from(mergedDateSet), now);
}

function getAvailablePickupTimesForDate(dateStr, now = new Date()) {
  const normalizedDate = normalizeYmdDate(dateStr);

  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedDate)) {
    return PICKUP_TIMES;
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

  // 「30分を切ったら変更不可」
  return minutes < CHANGE_LIMIT_MINUTES;
}

function buildReservationChangeLockedMessage(reservation) {
  const dateLabel = reservation?.date
    ? formatDateWithWeekday(reservation.date)
    : '-';
  const timeLabel = reservation?.time || '-';

  return textMessage(
    `お受け取りまでのお時間が30分を切っておりますので変更が出来ません💦\n\n` +
      `現在のご予約\n` +
      `受取日：${dateLabel}\n` +
      `受取時間：${timeLabel}`
  );
}
function jstDateTimeToUtcDate(dateStr, timeStr) {
  const [year, month, day] = normalizeYmdDate(dateStr).split('-').map(Number);
  const [hour, minute] = String(timeStr || '00:00').split(':').map(Number);

  return new Date(Date.UTC(year, month - 1, day, (hour || 0) - 9, minute || 0, 0));
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

function normalizeWebhookText(text) {
  return String(text || '')
    .normalize('NFKC')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .trim();
}

function normalizeIncomingText(text) {
  return String(text || '')
    .normalize('NFKC')
    .replace(/\s+/g, '')
    .replace(/[！!？?。．、,，]/g, '')
    .trim();
}

function normalizeYmdDate(value) {
  return String(value || '').replace(/\s+/g, '').trim();
}

function isReservationStartText(text) {
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
  const [year, month, day] = normalizeYmdDate(ymd).split('-').map(Number);
  return new Date(Date.UTC(year, month - 1, day));
}

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[BOOT ${APP_VERSION}] Server running on port ${PORT}`);
  console.log(`[BOOT ${APP_VERSION}] file=${__filename}`);
  console.log(`[BOOT ${APP_VERSION}] cwd=${process.cwd()}`);
});
