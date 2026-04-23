require("dotenv").config();

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const express = require("express");
const cors = require("cors");

const app = express();
const PORT = process.env.PORT || 3001;
const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID || "u1";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "admin-change-me";
const APP_URL = process.env.APP_URL || "https://cashtate.app";
const COMMISSION_RATE = Number(process.env.COMMISSION_RATE || 0.01);
const UNVERIFIED_DEPOSIT_LIMIT_USD = Number(process.env.UNVERIFIED_DEPOSIT_LIMIT_USD || 1000);
const DATA_DIR = path.join(__dirname, "data");
const DB_PATH = path.join(DATA_DIR, "db.json");

const ASSETS = ["RUB", "UAH", "USD", "EUR", "BTC", "USDT", "TRX", "SOL", "ETH"];
const EXCHANGE_COMMISSION_RATE = 0.001;
const USD_RATES = {
  USD: 1,
  EUR: 1.08,
  USDT: 1,
  RUB: 0.011,
  UAH: 0.026,
  BTC: 65000,
  ETH: 3200,
  SOL: 145,
  TRX: 0.12,
};

const DEPOSIT_CHANNELS = [
  {
    id: "banks_rf",
    title: "Банки РФ",
    currencies: ["RUB"],
    providers: ["Сбербанк", "Т-Банк", "Альфа-Банк", "ВТБ", "Газпромбанк", "Райффайзен", "ЮMoney", "СБП"],
  },
  {
    id: "banks_ua",
    title: "Банки Украины",
    currencies: ["UAH"],
    providers: ["ПриватБанк", "monobank", "ПУМБ", "Ощадбанк", "А-Банк", "УкрСиббанк", "Sense Bank"],
  },
  {
    id: "banks_eu",
    title: "Банки Европы",
    currencies: ["EUR"],
    providers: ["SEPA", "Revolut", "Wise", "N26", "Monese", "Paysera", "Santander", "Deutsche Bank"],
  },
  {
    id: "banks_cis",
    title: "Банки СНГ",
    currencies: ["USD", "EUR", "RUB"],
    providers: ["Kaspi", "Halyk", "TBC Bank", "Bank of Georgia", "Ameriabank", "Inecobank", "Moldindconbank"],
  },
  {
    id: "global_banks",
    title: "Крупные банки мира",
    currencies: ["USD", "EUR"],
    providers: ["SWIFT", "Wise", "Revolut", "Payoneer", "Chase", "Bank of America", "HSBC", "Barclays"],
  },
  {
    id: "retail_networks",
    title: "Платёжные сети и кассы",
    currencies: ["USD", "EUR", "RUB", "UAH"],
    providers: ["Western Union", "MoneyGram", "Ria", "KoronaPay", "Золотая Корона", "Contact", "PayPal"],
  },
  { id: "usdt_trc20", title: "USDT TRC-20", currencies: ["USDT"], providers: ["TRC-20"] },
  { id: "bitcoin", title: "Bitcoin", currencies: ["BTC"], providers: ["BTC"] },
  { id: "ethereum", title: "Ethereum", currencies: ["ETH", "USDT"], providers: ["ERC-20", "Ethereum Mainnet"] },
  { id: "solana", title: "Solana", currencies: ["SOL", "USDT"], providers: ["Solana SPL"] },
];

const CASH_TATE_PLANS = [
  { id: "six_months", title: "CashTate 6 месяцев", months: 6, monthlyRate: 0.15, directReferralRate: 0.05, level2Rate: 0.02, level3Rate: 0.01 },
  { id: "twelve_months", title: "CashTate 12 месяцев", months: 12, monthlyRate: 0.18, directReferralRate: 0.07, level2Rate: 0.03, level3Rate: 0.02 },
];

const KYC_COUNTRIES = {
  RU: {
    title: "Россия",
    documents: ["internal_passport", "foreign_passport", "driver_license"],
  },
  UA: {
    title: "Украина",
    documents: ["id_card", "foreign_passport", "driver_license"],
  },
  PL: {
    title: "Польша",
    documents: ["id_card", "passport", "driver_license", "residence_permit"],
  },
  DE: {
    title: "Германия",
    documents: ["id_card", "passport", "driver_license", "residence_permit"],
  },
  US: {
    title: "США",
    documents: ["passport", "driver_license", "state_id"],
  },
  GB: {
    title: "Великобритания",
    documents: ["passport", "driver_license", "residence_permit"],
  },
  KZ: {
    title: "Казахстан",
    documents: ["id_card", "passport", "driver_license"],
  },
  AM: {
    title: "Армения",
    documents: ["passport", "id_card", "driver_license"],
  },
  GE: {
    title: "Грузия",
    documents: ["passport", "id_card", "driver_license", "residence_permit"],
  },
  MD: {
    title: "Молдова",
    documents: ["passport", "id_card", "driver_license"],
  },
  OTHER: {
    title: "Другая страна",
    documents: ["passport", "id_card", "driver_license", "residence_permit"],
  },
};

const DOCUMENT_LABELS = {
  internal_passport: "Паспорт-книжка",
  foreign_passport: "Загранпаспорт",
  passport: "Паспорт",
  id_card: "ID-карта",
  driver_license: "Водительское удостоверение",
  residence_permit: "ВНЖ / Residence permit",
  state_id: "State ID",
};

const defaultDb = {
  users: {},
  sessions: {},
  depositMethods: [
    {
      id: "rf-card-default",
      channel: "banks_rf",
      type: "card",
      title: "Карта для RUB",
      value: "0000 0000 0000 0000",
      holder: "CASHTATE",
      active: true,
      note: "Реквизит назначается админом на конкретную заявку.",
    },
    {
      id: "usdt-trc20-default",
      channel: "usdt_trc20",
      type: "crypto",
      title: "USDT TRC-20",
      value: "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
      holder: "USDT",
      active: true,
      note: "Отправляйте только USDT в сети TRC-20.",
    },
  ],
  depositRequests: [],
  withdrawalRequests: [],
  kycRequests: [],
  exchangeRequests: [],
  cashTateDeposits: [],
  referralRewards: [],
  auditLog: [],
  ratesCache: null,
};

app.use(cors());
app.use(express.json({ limit: "25mb" }));

function ensureDb() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(DB_PATH)) {
    fs.writeFileSync(DB_PATH, JSON.stringify(defaultDb, null, 2));
  }
}

function readDb() {
  ensureDb();
  return JSON.parse(fs.readFileSync(DB_PATH, "utf8"));
}

function writeDb(db) {
  ensureDb();
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2));
}

function now() {
  return new Date().toISOString();
}

function id(prefix) {
  return `${prefix}_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
}

function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  const hash = crypto.pbkdf2Sync(String(password), salt, 120000, 64, "sha512").toString("hex");
  return `${salt}:${hash}`;
}

function verifyPassword(password, storedHash) {
  if (!storedHash) return false;
  const [salt, hash] = storedHash.split(":");
  return hashPassword(password, salt) === `${salt}:${hash}`;
}

function makeReferralCode(userId) {
  return `CT-${String(userId || "u1").replace(/\W/g, "").toUpperCase()}`;
}

function makeReferralLink(userId) {
  return `${APP_URL}?ref=${makeReferralCode(userId)}`;
}

function baseBalances() {
  return Object.fromEntries(ASSETS.map((asset) => [asset, 0]));
}

function toUsd(amount, currency) {
  return Number(amount || 0) * Number(USD_RATES[currency] || 1);
}

function approvedDepositUsd(db, userId) {
  return db.depositRequests
    .filter((request) => request.userId === userId && request.status === "approved")
    .reduce((sum, request) => sum + toUsd(request.amount, request.currency), 0);
}

async function getRates(db) {
  const cached = db.ratesCache;
  if (cached && Date.now() - cached.fetchedAt < 60 * 1000) return cached;

  const rates = { ...USD_RATES };
  let source = "fallback";

  try {
    const [cryptoResponse, fiatResponse] = await Promise.all([
      fetch(
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,tron,tether&vs_currencies=usd"
      ),
      fetch("https://api.frankfurter.app/latest?from=USD&to=EUR,UAH,RUB"),
    ]);
    const crypto = await cryptoResponse.json();
    const fiat = await fiatResponse.json();

    rates.BTC = Number(crypto.bitcoin?.usd || rates.BTC);
    rates.ETH = Number(crypto.ethereum?.usd || rates.ETH);
    rates.SOL = Number(crypto.solana?.usd || rates.SOL);
    rates.TRX = Number(crypto.tron?.usd || rates.TRX);
    rates.USDT = Number(crypto.tether?.usd || rates.USDT);
    rates.EUR = 1 / Number(fiat.rates?.EUR || 0.925);
    rates.UAH = 1 / Number(fiat.rates?.UAH || 38.5);
    rates.RUB = 1 / Number(fiat.rates?.RUB || 90);
    source = "coingecko+frankfurter";
  } catch (err) {
    console.error("Rates fetch failed:", err.message);
  }

  db.ratesCache = { rates, source, fetchedAt: Date.now(), fetchedAtIso: now() };
  writeDb(db);
  return db.ratesCache;
}

function getReferralChain(db, userId) {
  const chain = [];
  let user = db.users[userId];

  for (let i = 0; i < 3; i += 1) {
    if (!user?.invitedBy) break;
    const inviter = Object.values(db.users).find((item) => item.referralCode === user.invitedBy);
    if (!inviter) break;
    chain.push(inviter);
    user = inviter;
  }

  return chain;
}

function publicUser(user) {
  return {
    id: user.id,
    telegramId: user.telegramId,
    firstName: user.firstName,
    username: user.username,
    role: user.role,
    isAdmin: user.role === "admin",
    status: user.status || "active",
    profile: user.profile,
    balances: user.balances,
    referralCode: user.referralCode,
    referralLink: makeReferralLink(user.telegramId),
    kycStatus: user.kycStatus || "not_started",
    createdAt: user.createdAt,
  };
}

function audit(db, actorId, action, payload = {}) {
  db.auditLog.unshift({
    id: id("audit"),
    actorId,
    action,
    payload,
    createdAt: now(),
  });
}

function getToken(req) {
  const header = req.headers.authorization || "";
  return header.startsWith("Bearer ") ? header.slice(7) : null;
}

function auth(req, res, next) {
  const db = readDb();
  const token = getToken(req);
  const session = token ? db.sessions[token] : null;
  const user = session ? db.users[session.userId] : null;

  if (!user) return res.status(401).json({ ok: false, message: "Unauthorized" });
  if (user.status === "banned" || user.status === "blocked") {
    return res.status(403).json({ ok: false, message: "Account is blocked" });
  }

  req.db = db;
  req.user = user;
  req.token = token;
  next();
}

function adminOnly(req, res, next) {
  if (req.user.role !== "admin") {
    return res.status(403).json({ ok: false, message: "Admin access required" });
  }

  next();
}

function upsertUser(db, payload) {
  const telegramId = String(payload.telegramId || "demo_user");
  const existing = db.users[telegramId];
  const role = telegramId === String(ADMIN_TELEGRAM_ID) ? "admin" : existing?.role || "user";

  const user = {
    id: telegramId,
    telegramId,
    firstName: payload.firstName || existing?.firstName || "",
    username: payload.username || existing?.username || "",
    passwordHash: existing?.passwordHash || null,
    role,
    status: existing?.status || "active",
    profile: {
      fullName: "",
      phone: "",
      email: "",
      country: "",
      city: "",
      ...existing?.profile,
      ...payload.profile,
    },
    balances: { ...baseBalances(), ...existing?.balances },
    referralCode: makeReferralCode(telegramId),
    invitedBy: existing?.invitedBy || payload.referralCode || null,
    kycStatus: existing?.kycStatus || "not_started",
    createdAt: existing?.createdAt || now(),
    updatedAt: now(),
  };

  db.users[telegramId] = user;
  return user;
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    message: "CashTate API is running",
    features: ["auth", "admin", "deposits", "withdrawals", "kyc", "balances"],
  });
});

app.get("/meta", (req, res) => {
  res.json({
    assets: ASSETS,
    commissionRate: COMMISSION_RATE,
    exchangeCommissionRate: EXCHANGE_COMMISSION_RATE,
    unverifiedDepositLimitUsd: UNVERIFIED_DEPOSIT_LIMIT_USD,
    usdRates: USD_RATES,
    depositChannels: DEPOSIT_CHANNELS,
    withdrawalChannels: DEPOSIT_CHANNELS,
    cashTatePlans: CASH_TATE_PLANS,
    kycCountries: KYC_COUNTRIES,
    documentLabels: DOCUMENT_LABELS,
    rules: {
      commission: "Комиссия сервиса на пополнение и вывод составляет 1%.",
      unverified:
        "Без KYC можно создавать заявки на пополнение до эквивалента 1000 USD, но вывод средств недоступен.",
      verified:
        "После успешной KYC-верификации пользователю доступен вывод и повышенные лимиты по решению администратора.",
      aml:
        "Администратор может запросить дополнительные документы, источник средств и отклонить операцию при AML/KYC рисках.",
    },
  });
});

app.post("/auth/register", (req, res) => {
  const { telegramId, password, referralCode } = req.body;
  if (!telegramId || !password) {
    return res.status(400).json({ ok: false, message: "telegramId and password are required" });
  }

  const db = readDb();
  const existing = db.users[String(telegramId)];
  const user = upsertUser(db, req.body);

  if (!existing?.passwordHash) {
    user.passwordHash = hashPassword(password);
  }

  if (referralCode && !existing?.invitedBy) {
    const inviter = Object.values(db.users).find((item) => item.referralCode === referralCode);
    if (inviter && inviter.telegramId !== user.telegramId) {
      user.invitedBy = referralCode;
    }
  }

  const token = crypto.randomBytes(32).toString("hex");
  db.sessions[token] = { userId: user.telegramId, createdAt: now() };
  audit(db, user.telegramId, "auth.register");
  writeDb(db);

  res.json({ ok: true, token, user: publicUser(user) });
});

app.post("/auth/login", (req, res) => {
  const { telegramId, password } = req.body;
  const db = readDb();

  if (String(telegramId) === String(ADMIN_TELEGRAM_ID) && password === ADMIN_PASSWORD) {
    const user = upsertUser(db, {
      telegramId,
      firstName: "Admin",
      username: "admin",
    });
    user.passwordHash = hashPassword(password);

    const token = crypto.randomBytes(32).toString("hex");
    db.sessions[token] = { userId: user.telegramId, createdAt: now() };
    audit(db, user.telegramId, "auth.admin_login");
    writeDb(db);
    return res.json({ ok: true, token, user: publicUser(user) });
  }

  const user = db.users[String(telegramId)];
  if (!user || !verifyPassword(password, user.passwordHash)) {
    return res.status(401).json({ ok: false, message: "Wrong Telegram ID or password" });
  }

  const token = crypto.randomBytes(32).toString("hex");
  db.sessions[token] = { userId: user.telegramId, createdAt: now() };
  audit(db, user.telegramId, "auth.login");
  writeDb(db);

  res.json({ ok: true, token, user: publicUser(user) });
});

app.post("/register", (req, res) => {
  const db = readDb();
  const user = upsertUser(db, req.body);
  audit(db, user.telegramId, "telegram.register");
  writeDb(db);
  res.json({ ok: true, user: publicUser(user), referralLink: makeReferralLink(user.telegramId) });
});

app.get("/me", auth, (req, res) => {
  res.json({ ok: true, user: publicUser(req.user) });
});

app.patch("/me/profile", auth, (req, res) => {
  const db = req.db;
  const user = db.users[req.user.telegramId];
  user.profile = { ...user.profile, ...req.body };
  user.updatedAt = now();
  audit(db, user.telegramId, "profile.update");
  writeDb(db);
  res.json({ ok: true, user: publicUser(user) });
});

app.get("/wallets/:userId", (req, res) => {
  const db = readDb();
  const user = db.users[String(req.params.userId)];
  const balances = user?.balances || baseBalances();
  res.json(ASSETS.map((asset) => ({ asset, balanceAvailable: balances[asset] || 0 })));
});

app.get("/rates", auth, async (req, res) => {
  const rates = await getRates(req.db);
  res.json(rates);
});

app.get("/referrals/:userId", (req, res) => {
  const db = readDb();
  const userId = String(req.params.userId || "u1");
  const referralCode = makeReferralCode(userId);
  const invitedUsers = Object.values(db.users)
    .filter((user) => user.invitedBy === referralCode)
    .map((user) => ({ id: user.id, firstName: user.firstName, username: user.username }));

  res.json({ referralCode, referralLink: makeReferralLink(userId), invitedUsers });
});

app.get("/deposit-methods", auth, (req, res) => {
  res.json(req.db.depositMethods.filter((method) => method.active));
});

app.get("/admin/deposit-methods", auth, adminOnly, (req, res) => {
  res.json(req.db.depositMethods);
});

app.post("/admin/deposit-methods", auth, adminOnly, (req, res) => {
  const { channel, type, title, value, holder = "", active = true, note = "" } = req.body;
  if (!channel || !type || !title || !value) {
    return res.status(400).json({ ok: false, message: "channel, type, title and value are required" });
  }

  const method = { id: id("method"), channel, type, title, value, holder, active: Boolean(active), note };
  req.db.depositMethods.unshift(method);
  audit(req.db, req.user.telegramId, "deposit_method.create", { methodId: method.id });
  writeDb(req.db);
  res.status(201).json(method);
});

app.patch("/admin/deposit-methods/:id", auth, adminOnly, (req, res) => {
  const method = req.db.depositMethods.find((item) => item.id === req.params.id);
  if (!method) return res.status(404).json({ ok: false, message: "Deposit method not found" });

  Object.assign(method, req.body, { id: method.id });
  audit(req.db, req.user.telegramId, "deposit_method.update", { methodId: method.id });
  writeDb(req.db);
  res.json(method);
});

app.delete("/admin/deposit-methods/:id", auth, adminOnly, (req, res) => {
  req.db.depositMethods = req.db.depositMethods.filter((item) => item.id !== req.params.id);
  audit(req.db, req.user.telegramId, "deposit_method.delete", { methodId: req.params.id });
  writeDb(req.db);
  res.json({ ok: true });
});

app.get("/deposit-requests", auth, (req, res) => {
  const requests =
    req.user.role === "admin"
      ? req.db.depositRequests
      : req.db.depositRequests.filter((item) => item.userId === req.user.telegramId);
  res.json(requests);
});

app.post("/deposit-requests", auth, (req, res) => {
  const { amount, currency, channel, provider = "", comment = "" } = req.body;
  if (!amount || Number(amount) <= 0 || !currency || !channel) {
    return res.status(400).json({ ok: false, message: "amount, currency and channel are required" });
  }

  const amountUsd = toUsd(amount, currency);
  const alreadyDepositedUsd = approvedDepositUsd(req.db, req.user.telegramId);
  if (req.user.kycStatus !== "verified" && alreadyDepositedUsd + amountUsd > UNVERIFIED_DEPOSIT_LIMIT_USD) {
    return res.status(400).json({
      ok: false,
      message: `Без KYC лимит пополнения до ${UNVERIFIED_DEPOSIT_LIMIT_USD} USD в эквиваленте`,
    });
  }

  const feeAmount = Number(amount) * COMMISSION_RATE;
  const creditAmount = Number(amount) - feeAmount;

  const request = {
    id: id("dep"),
    userId: req.user.telegramId,
    userName: req.user.firstName || req.user.username || req.user.telegramId,
    amount: Number(amount),
    feeAmount,
    creditAmount,
    commissionRate: COMMISSION_RATE,
    currency,
    channel,
    provider,
    comment,
    status: "awaiting_requisites",
    assignedRequisites: null,
    proof: null,
    adminNote: "",
    createdAt: now(),
    updatedAt: now(),
  };

  req.db.depositRequests.unshift(request);
  audit(req.db, req.user.telegramId, "deposit_request.create", { requestId: request.id });
  writeDb(req.db);
  res.status(201).json(request);
});

app.patch("/admin/deposit-requests/:id", auth, adminOnly, (req, res) => {
  const request = req.db.depositRequests.find((item) => item.id === req.params.id);
  if (!request) return res.status(404).json({ ok: false, message: "Deposit request not found" });

  const { status, assignedRequisites, adminNote } = req.body;
  if (assignedRequisites) request.assignedRequisites = assignedRequisites;
  if (adminNote !== undefined) request.adminNote = adminNote;
  if (status) request.status = status;
  request.updatedAt = now();

  if (status === "approved") {
    const user = req.db.users[request.userId];
    user.balances[request.currency] =
      Number(user.balances[request.currency] || 0) + Number(request.creditAmount || request.amount);
    user.updatedAt = now();
  }

  audit(req.db, req.user.telegramId, "deposit_request.admin_update", {
    requestId: request.id,
    status: request.status,
  });
  writeDb(req.db);
  res.json(request);
});

app.post("/deposit-requests/:id/proof", auth, (req, res) => {
  const request = req.db.depositRequests.find(
    (item) => item.id === req.params.id && item.userId === req.user.telegramId
  );
  if (!request) return res.status(404).json({ ok: false, message: "Deposit request not found" });

  request.proof = {
    fileName: req.body.fileName || "payment-proof",
    fileType: req.body.fileType || "application/octet-stream",
    dataUrl: req.body.dataUrl || "",
    comment: req.body.comment || "",
    uploadedAt: now(),
  };
  request.status = "proof_submitted";
  request.updatedAt = now();
  audit(req.db, req.user.telegramId, "deposit_request.proof_upload", { requestId: request.id });
  writeDb(req.db);
  res.json(request);
});

app.get("/withdrawal-requests", auth, (req, res) => {
  const requests =
    req.user.role === "admin"
      ? req.db.withdrawalRequests
      : req.db.withdrawalRequests.filter((item) => item.userId === req.user.telegramId);
  res.json(requests);
});

app.post("/withdrawal-requests", auth, (req, res) => {
  const { amount, currency, method, provider = "", details, comment = "" } = req.body;
  if (!amount || Number(amount) <= 0 || !currency || !method || !details) {
    return res.status(400).json({ ok: false, message: "amount, currency, method and details are required" });
  }

  const user = req.db.users[req.user.telegramId];
  if (user.kycStatus !== "verified") {
    return res.status(403).json({ ok: false, message: "Вывод доступен только после успешной KYC-верификации" });
  }

  const feeAmount = Number(amount) * COMMISSION_RATE;
  const debitAmount = Number(amount) + feeAmount;
  if (Number(user.balances[currency] || 0) < debitAmount) {
    return res.status(400).json({ ok: false, message: "Insufficient balance" });
  }

  const request = {
    id: id("wd"),
    userId: req.user.telegramId,
    userName: req.user.firstName || req.user.username || req.user.telegramId,
    amount: Number(amount),
    feeAmount,
    debitAmount,
    commissionRate: COMMISSION_RATE,
    currency,
    method,
    provider,
    details,
    comment,
    status: "pending",
    adminNote: "",
    createdAt: now(),
    updatedAt: now(),
  };

  req.db.withdrawalRequests.unshift(request);
  audit(req.db, req.user.telegramId, "withdrawal_request.create", { requestId: request.id });
  writeDb(req.db);
  res.status(201).json(request);
});

app.patch("/admin/withdrawal-requests/:id", auth, adminOnly, (req, res) => {
  const request = req.db.withdrawalRequests.find((item) => item.id === req.params.id);
  if (!request) return res.status(404).json({ ok: false, message: "Withdrawal request not found" });

  const { status, adminNote = "" } = req.body;
  if (status === "approved" && request.status !== "approved") {
    const user = req.db.users[request.userId];
    user.balances[request.currency] =
      Number(user.balances[request.currency] || 0) - Number(request.debitAmount || request.amount);
  }

  request.status = status || request.status;
  request.adminNote = adminNote;
  request.updatedAt = now();
  audit(req.db, req.user.telegramId, "withdrawal_request.admin_update", {
    requestId: request.id,
    status: request.status,
  });
  writeDb(req.db);
  res.json(request);
});

app.post("/exchange", auth, async (req, res) => {
  const { fromAsset, toAsset, amount } = req.body;
  const numericAmount = Number(amount);

  if (!fromAsset || !toAsset || fromAsset === toAsset || !numericAmount || numericAmount <= 0) {
    return res.status(400).json({ ok: false, message: "fromAsset, toAsset and positive amount are required" });
  }

  const user = req.db.users[req.user.telegramId];
  if (Number(user.balances[fromAsset] || 0) < numericAmount) {
    return res.status(400).json({ ok: false, message: "Insufficient balance" });
  }

  const { rates } = await getRates(req.db);
  const grossToAmount = (numericAmount * Number(rates[fromAsset] || 1)) / Number(rates[toAsset] || 1);
  const feeAmount = grossToAmount * EXCHANGE_COMMISSION_RATE;
  const receivedAmount = grossToAmount - feeAmount;

  user.balances[fromAsset] = Number(user.balances[fromAsset] || 0) - numericAmount;
  user.balances[toAsset] = Number(user.balances[toAsset] || 0) + receivedAmount;

  const exchange = {
    id: id("ex"),
    userId: user.telegramId,
    fromAsset,
    toAsset,
    amount: numericAmount,
    grossToAmount,
    feeAmount,
    receivedAmount,
    commissionRate: EXCHANGE_COMMISSION_RATE,
    createdAt: now(),
  };

  req.db.exchangeRequests.unshift(exchange);
  audit(req.db, user.telegramId, "exchange.create", { exchangeId: exchange.id });
  writeDb(req.db);
  res.status(201).json(exchange);
});

app.get("/exchange", auth, (req, res) => {
  const requests =
    req.user.role === "admin"
      ? req.db.exchangeRequests
      : req.db.exchangeRequests.filter((item) => item.userId === req.user.telegramId);
  res.json(requests);
});

app.get("/cashtate", auth, (req, res) => {
  const deposits =
    req.user.role === "admin"
      ? req.db.cashTateDeposits
      : req.db.cashTateDeposits.filter((item) => item.userId === req.user.telegramId);
  const rewards =
    req.user.role === "admin"
      ? req.db.referralRewards
      : req.db.referralRewards.filter((item) => item.userId === req.user.telegramId);
  res.json({ plans: CASH_TATE_PLANS, deposits, rewards });
});

app.post("/cashtate/deposits", auth, (req, res) => {
  const { planId, asset, amount } = req.body;
  const plan = CASH_TATE_PLANS.find((item) => item.id === planId);
  const numericAmount = Number(amount);
  const user = req.db.users[req.user.telegramId];

  if (!plan || !asset || !numericAmount || numericAmount <= 0) {
    return res.status(400).json({ ok: false, message: "planId, asset and positive amount are required" });
  }
  if (Number(user.balances[asset] || 0) < numericAmount) {
    return res.status(400).json({ ok: false, message: "Insufficient balance" });
  }

  user.balances[asset] = Number(user.balances[asset] || 0) - numericAmount;
  const startedAt = new Date();
  const lockedUntil = new Date(startedAt);
  lockedUntil.setMonth(lockedUntil.getMonth() + plan.months);

  const deposit = {
    id: id("ct"),
    userId: user.telegramId,
    userName: user.firstName || user.username || user.telegramId,
    planId: plan.id,
    asset,
    amount: numericAmount,
    monthlyRate: plan.monthlyRate,
    expectedMonthlyProfit: numericAmount * plan.monthlyRate,
    status: "active",
    startedAt: startedAt.toISOString(),
    lockedUntil: lockedUntil.toISOString(),
  };
  req.db.cashTateDeposits.unshift(deposit);

  const chain = getReferralChain(req.db, user.telegramId);
  const rates = [plan.directReferralRate, plan.level2Rate, plan.level3Rate];
  chain.forEach((inviter, index) => {
    req.db.referralRewards.unshift({
      id: id("rew"),
      userId: inviter.telegramId,
      sourceUserId: user.telegramId,
      cashTateDepositId: deposit.id,
      level: index + 1,
      asset,
      rate: rates[index],
      fromMonthlyProfit: deposit.expectedMonthlyProfit,
      amount: deposit.expectedMonthlyProfit * rates[index],
      status: "accrual_projection",
      createdAt: now(),
    });
  });

  audit(req.db, user.telegramId, "cashtate.deposit_create", { depositId: deposit.id });
  writeDb(req.db);
  res.status(201).json(deposit);
});

app.get("/kyc", auth, (req, res) => {
  const requests =
    req.user.role === "admin"
      ? req.db.kycRequests
      : req.db.kycRequests.filter((item) => item.userId === req.user.telegramId);
  res.json(requests);
});

app.post("/kyc", auth, (req, res) => {
  const { country, documentType, documentNumber, address, files = {} } = req.body;
  if (!country || !documentType || !documentNumber) {
    return res.status(400).json({ ok: false, message: "country, documentType and documentNumber are required" });
  }

  const request = {
    id: id("kyc"),
    userId: req.user.telegramId,
    userName: req.user.firstName || req.user.username || req.user.telegramId,
    country,
    documentType,
    documentNumber,
    address: address || "",
    files,
    status: "pending",
    adminNote: "",
    createdAt: now(),
    updatedAt: now(),
  };

  req.db.kycRequests.unshift(request);
  req.db.users[req.user.telegramId].kycStatus = "pending";
  audit(req.db, req.user.telegramId, "kyc.submit", { requestId: request.id });
  writeDb(req.db);
  res.status(201).json(request);
});

app.patch("/admin/kyc/:id", auth, adminOnly, (req, res) => {
  const request = req.db.kycRequests.find((item) => item.id === req.params.id);
  if (!request) return res.status(404).json({ ok: false, message: "KYC request not found" });

  request.status = req.body.status || request.status;
  request.adminNote = req.body.adminNote || "";
  request.updatedAt = now();
  req.db.users[request.userId].kycStatus = request.status;
  audit(req.db, req.user.telegramId, "kyc.admin_update", {
    requestId: request.id,
    status: request.status,
  });
  writeDb(req.db);
  res.json(request);
});

app.patch("/admin/users/:id", auth, adminOnly, (req, res) => {
  const user = req.db.users[String(req.params.id)];
  if (!user) return res.status(404).json({ ok: false, message: "User not found" });

  const { status, balancePatch, adminNote = "" } = req.body;
  if (status) user.status = status;

  if (balancePatch?.asset && Number.isFinite(Number(balancePatch.amount))) {
    const asset = balancePatch.asset;
    user.balances[asset] = Number(user.balances[asset] || 0) + Number(balancePatch.amount);
  }

  user.adminNote = adminNote || user.adminNote || "";
  user.updatedAt = now();
  audit(req.db, req.user.telegramId, "admin.user_update", {
    targetUserId: user.telegramId,
    status: user.status,
    balancePatch,
  });
  writeDb(req.db);
  res.json(publicUser(user));
});

app.get("/admin/overview", auth, adminOnly, (req, res) => {
  const users = Object.values(req.db.users);
  res.json({
    users: users.map(publicUser),
    depositRequests: req.db.depositRequests,
    withdrawalRequests: req.db.withdrawalRequests,
    kycRequests: req.db.kycRequests,
    exchangeRequests: req.db.exchangeRequests,
    cashTateDeposits: req.db.cashTateDeposits,
    referralRewards: req.db.referralRewards,
    depositMethods: req.db.depositMethods,
    auditLog: req.db.auditLog.slice(0, 100),
    stats: {
      users: users.length,
      pendingDeposits: req.db.depositRequests.filter((item) => !["approved", "rejected"].includes(item.status)).length,
      pendingWithdrawals: req.db.withdrawalRequests.filter((item) => item.status === "pending").length,
      pendingKyc: req.db.kycRequests.filter((item) => item.status === "pending").length,
    },
  });
});

app.listen(PORT, () => {
  ensureDb();
  console.log(`CashTate API running on http://localhost:${PORT}`);
  console.log(`Admin Telegram ID: ${ADMIN_TELEGRAM_ID}`);
});
