'use strict';

const paths = {
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6m0-10v.1"/>',
  refresh: '<path d="M20 7v5h-5M4 17v-5h5"/><path d="M6.1 6.1A8 8 0 0 1 20 12M4 12a8 8 0 0 0 13.9 5.9"/>',
  search: '<circle cx="10.8" cy="10.8" r="6.8"/><path d="m16 16 4.5 4.5"/>',
  'chevron-left': '<path d="m14 6-6 6 6 6"/>',
  'chevron-right': '<path d="m10 6 6 6-6 6"/>',
  layers: '<path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
  power: '<path d="M12 3v8m-6.4-5.1a9 9 0 1 0 12.8 0"/>',
  activity: '<path d="M2 12h5l3-8 4 16 3-8h5"/>',
  close: '<path d="m6 6 12 12M6 18 18 6"/>',
  'arrow-right': '<path d="M4 12h16m-6-6 6 6-6 6"/>',
  pause: '<path d="M8 5v14M16 5v14"/>',
  play: '<path d="m8 4 12 8-12 8V4Z"/>',
  download: '<path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5"/>',
  expand: '<path d="M14 4h6v6M4 14v6h6m10-16-7 7M4 20l7-7"/>',
  orders: '<path d="M8 4h12v16H4V8m0-4h4v4H4V4Zm4 8h8m-8 4h8"/>',
  check: '<path d="m5 12 4 4L19 6"/>',
  copy: '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V4H4v12h4"/>',
};
const icon = name => `<svg viewBox="0 0 24 24" aria-hidden="true">${paths[name] || paths.info}</svg>`;
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number = value => new Intl.NumberFormat('en-US', {notation: Math.abs(Number(value)) >= 1e6 ? 'compact' : 'standard', maximumFractionDigits: 1}).format(value || 0);
const timeText = value => value ? new Date(value).toLocaleTimeString('en-GB', {timeZone:'UTC', hour12:false}) : '—';
const className = value => String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, '-');
const OUTRIGHT_PAGE_SIZE = 8;
const ui = {state:null, csrf:'', cursor:0, logs:[], products:[], page:0, search:'', focused:'alice', loginSlot:'alice', failures:0, journalPaused:false, journalFrozen:null, journalPage:0,
  panels:{alice:{side:'Buy',tab:'orders',page:0,paused:false,revision:-1}, bob:{side:'Sell',tab:'orders',page:0,paused:false,revision:-1}}, marketKey:'', systemKey:''};

// Size pages from available panel space, preserving the font size and all five levels.
function visibleCapacity(element, property, fallback) {
  const style = getComputedStyle(element);
  const height = element.clientHeight - (parseFloat(style.paddingTop) || 0) - (parseFloat(style.paddingBottom) || 0);
  return Math.max(1, Math.floor(height / (parseFloat(style.getPropertyValue(property)) || fallback)));
}
function pageSlice(items, page, capacity) {
  const pages = Math.max(1, Math.ceil(items.length / capacity));
  page = Math.max(0, Math.min(page, pages - 1));
  const start = page * capacity;
  return {page, pages, start, items:items.slice(start, start + capacity), total:items.length};
}
function updatePager(previous, next, label, page) {
  $(previous).disabled = page.page === 0;
  $(next).disabled = page.page >= page.pages - 1;
  $(label).textContent = `${page.page + 1} / ${page.pages}`;
  $(label).title = page.total ? `${page.start + 1}–${page.start + page.items.length} of ${page.total}` : 'No records';
}

function hydrateIcons(root=document) { root.querySelectorAll('[data-icon]').forEach(el => {el.innerHTML = icon(el.dataset.icon);}); }
hydrateIcons();

function toast(message, level='success') {
  const element = document.createElement('div');
  element.className = `toast ${level}`;
  element.textContent = message;
  $('toasts').append(element);
  setTimeout(() => element.remove(), level === 'error' ? 6500 : 3500);
}

async function api(path, body) {
  if (!ui.csrf) throw new Error('Waiting for the local console connection');
  const response = await fetch(path, {method:'POST', headers:{'Content-Type':'application/json','X-Console-Token':ui.csrf}, body:JSON.stringify(body), signal:AbortSignal.timeout(10000)});
  const data = await response.json();
  if (!response.ok || !data.ok) throw new Error(data.error || 'Request failed');
  return data;
}

function empty(title, subtitle, name='orders') {
  return `<div class="empty-state"><span>${icon(name)}</span><p>${esc(title)}</p><small>${esc(subtitle)}</small></div>`;
}

function traderTemplate(slot) {
  const name = slot === 'alice' ? 'Alice' : 'Bob';
  const side = ui.panels[slot].side;
  return `<article class="panel trader ${slot}" id="${slot}-panel" aria-label="${name} trading panel" data-slot="${slot}">
    <div class="account-header"><div class="account-identity"><div class="avatar">${name[0]}</div><div><div class="account-name" id="${slot}-name">${name}</div><div class="account-state" id="${slot}-status"><span class="dot"></span>Logged out</div></div></div><button class="button login-button" id="${slot}-login">Log in</button></div>
    <form class="ticket" id="${slot}-form"><div class="side-toggle" role="group" aria-label="${name} order side"><button type="button" class="side-button buy ${side==='Buy'?'active':''}" data-side="Buy" aria-pressed="${side==='Buy'}">Buy</button><button type="button" class="side-button sell ${side==='Sell'?'active':''}" data-side="Sell" aria-pressed="${side==='Sell'}">Sell</button></div><label for="${slot}-symbol">Symbol<select id="${slot}-symbol" name="symbol" required></select></label><div class="ticket-grid"><label for="${slot}-price">Limit price<input id="${slot}-price" type="number" name="price" required step="1" inputmode="numeric" value="100"></label><label for="${slot}-quantity">Quantity<input id="${slot}-quantity" type="number" name="quantity" required min="1" max="2147483647" step="1" inputmode="numeric" value="1"></label></div><div class="order-hint"><span id="${slot}-limits">Limits —</span><span>Limit · GTC</span></div><button class="button submit-order ${side==='Sell'?'sell':''}" id="${slot}-submit" type="submit" disabled><span id="${slot}-submit-label">Log in to trade</span>${icon('arrow-right')}</button><div class="ticket-error" id="${slot}-error" role="alert"></div></form>
    <section class="activity-area" aria-label="${name} orders and trades"><div class="activity-tabs" role="tablist"><button class="activity-tab active" role="tab" aria-selected="true" data-tab="orders">Orders <span class="tab-count" id="${slot}-order-count">0</span></button><button class="activity-tab" role="tab" aria-selected="false" data-tab="history">History <span class="tab-count" id="${slot}-history-count">0</span></button><button class="activity-tab" role="tab" aria-selected="false" data-tab="fills">Fills <span class="tab-count" id="${slot}-fill-count">0</span></button></div><div class="activity-tools"><span id="${slot}-activity-caption">Active orders</span><div><button class="text-button" id="${slot}-query" title="Query committed trade history">${icon('refresh')} Query trades</button><a class="icon-button small-button" id="${slot}-export" href="/api/history/${slot}.csv" download="${slot}-trades.csv" aria-label="Export ${name} loaded trade history" title="Export loaded trade history" hidden>${icon('download')}</a></div></div><div class="activity-body" id="${slot}-activity" role="tabpanel">${empty('No active orders','Log in to view your exchange orders.')}</div><div class="history-footer" id="${slot}-history-footer" hidden><span id="${slot}-history-caption">No query yet</span><button class="text-button" id="${slot}-more">Load more →</button></div><form class="cancel-form" id="${slot}-cancel-form"><label class="sr-only" for="${slot}-cancel-id">${name} order ID to cancel</label><input id="${slot}-cancel-id" type="text" inputmode="numeric" pattern="[0-9]+" placeholder="Order ID to cancel" required autocomplete="off"><button class="button quiet" id="${slot}-cancel" type="submit" disabled>Cancel</button></form></section>
    <section class="trader-log" aria-label="${name} log"><div class="trader-log-heading"><span>${name} <span class="muted">/</span> Activity log</span><div><button class="icon-button" id="${slot}-pause-log" title="Pause log scrolling" aria-label="Pause ${name} log scrolling">${icon('pause')}</button><button class="icon-button log-expand" data-source="${slot}" title="Expand ${name} log" aria-label="Expand ${name} log">${icon('expand')}</button></div></div><div class="trader-log-output" id="${slot}-log" role="log" aria-label="${name} trading events"><div class="log-empty">Your session activity will appear here.</div></div></section>
  </article>`;
}
$('traders').innerHTML = ['alice','bob'].map(traderTemplate).join('');
for (const slot of ['alice','bob']) {
  $(`${slot}-activity`).insertAdjacentHTML('afterend', `<div class="activity-pagination pagination" aria-label="${slot} activity pages"><button class="icon-button small-button" id="${slot}-previous" aria-label="Previous ${slot} records">${icon('chevron-left')}</button><span id="${slot}-page-label">1 / 1</span><button class="icon-button small-button" id="${slot}-next" aria-label="Next ${slot} records">${icon('chevron-right')}</button></div>`);
  $(`${slot}-pause-log`).title = 'Pause live log';
  $(`${slot}-pause-log`).setAttribute('aria-label', `Pause ${slot} live log`);
}
$('journal-output').insertAdjacentHTML('afterend', `<div class="journal-pagination pagination"><span>Newest first · Hover for full text</span><div><button class="icon-button" id="journal-previous" aria-label="Newer log entries">${icon('chevron-left')}</button><span id="journal-page-label">1 / 1</span><button class="icon-button" id="journal-next" aria-label="Older log entries">${icon('chevron-right')}</button></div></div>`);

function setSide(slot, side) {
  ui.panels[slot].side = side;
  document.querySelectorAll(`#${slot}-form [data-side]`).forEach(button => {
    const active = button.dataset.side === side;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', active);
  });
  $(`${slot}-submit`).classList.toggle('sell', side === 'Sell');
  updateTicket(slot);
}

function updateTicket(slot, resetPrice=false) {
  const product = ui.products.find(p => p.symbol === $(`${slot}-symbol`).value);
  if (product) {
    $(`${slot}-price`).min = product.lower;
    $(`${slot}-price`).max = product.upper;
    if (resetPrice) $(`${slot}-price`).value = product.close;
    $(`${slot}-limits`).textContent = `Limits ${product.lower} – ${product.upper}`;
  }
  const ready = ui.state?.traders[slot].status === 'Ready';
  $(`${slot}-submit`).disabled = !ready;
  $(`${slot}-cancel`).disabled = !ready;
  $(`${slot}-submit-label`).textContent = ready ? `${ui.panels[slot].side} ${product?.symbol || 'order'}` : 'Log in to trade';
}

function selectSymbol(slot, symbol, price, side) {
  $(`${slot}-symbol`).value = symbol;
  updateTicket(slot, price === undefined);
  if (price !== undefined) $(`${slot}-price`).value = price;
  if (side) setSide(slot, side);
  ui.focused = slot;
  toast(`${symbol}${price !== undefined ? ` at ${price}` : ''} selected for ${slot === 'alice' ? 'Alice' : 'Bob'}`);
}

function openLogin(slot) {
  ui.loginSlot = slot;
  $('login-user').innerHTML = (ui.state?.accounts || ['alice','bob']).map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join('');
  $('login-user').value = ui.state?.traders[slot].username || slot;
  $('login-error').textContent = '';
  $('login-password').value = '';
  $('login-dialog').showModal();
}

function setTab(slot, tab) {
  ui.panels[slot].tab = tab;
  ui.panels[slot].page = 0;
  ui.panels[slot].revision = -1;
  document.querySelectorAll(`#${slot}-panel [data-tab]`).forEach(el => {
    const active = el.dataset.tab === tab;
    el.classList.toggle('active', active);
    el.setAttribute('aria-selected', active);
  });
  if (ui.state) renderActivity(slot, ui.state.traders[slot]);
}

for (const slot of ['alice','bob']) {
  const panel = $(`${slot}-panel`);
  panel.addEventListener('pointerdown', () => {ui.focused = slot;});
  panel.querySelectorAll('[data-side]').forEach(button => button.addEventListener('click', () => setSide(slot, button.dataset.side)));
  panel.querySelectorAll('[data-tab]').forEach(button => button.addEventListener('click', () => setTab(slot, button.dataset.tab)));
  $(`${slot}-symbol`).addEventListener('change', () => updateTicket(slot, true));
  $(`${slot}-login`).addEventListener('click', async () => {
    const active = ['Ready','Connecting','Synchronizing','Reconnecting'].includes(ui.state?.traders[slot].status);
    if (active) {
      try {await api('/api/logout',{slot}); toast(`${slot} logged out`);} catch(error) {toast(error.message,'error');}
    } else openLogin(slot);
  });
  $(`${slot}-form`).addEventListener('submit', async event => {
    event.preventDefault();
    $(`${slot}-error`).textContent = '';
    const button = $(`${slot}-submit`); button.disabled = true;
    try {
      await api('/api/order', {slot, symbol:$(`${slot}-symbol`).value, side:ui.panels[slot].side,
        price:Number($(`${slot}-price`).value), quantity:Number($(`${slot}-quantity`).value)});
      toast('Order sent · awaiting exchange response');
    } catch(error) {$(`${slot}-error`).textContent = error.message; toast(error.message,'error');}
    finally {updateTicket(slot);}
  });
  $(`${slot}-cancel-form`).addEventListener('submit', async event => {
    event.preventDefault();
    await cancelOrder(slot, $(`${slot}-cancel-id`).value.trim());
  });
  $(`${slot}-activity`).addEventListener('click', async event => {
    const cancel = event.target.closest('[data-cancel]');
    const detail = event.target.closest('[data-trade-id]');
    if (cancel) await cancelOrder(slot,cancel.dataset.cancel);
    if (detail) {
      const records = ui.panels[slot].tab === 'history' ? ui.state.traders[slot].history : ui.state.traders[slot].fills;
      const record = records.find(r => r.id === detail.dataset.tradeId && (!detail.dataset.tradeType || r.type === detail.dataset.tradeType));
      if (record) tradeDetails(record);
    }
  });
  $(`${slot}-query`).addEventListener('click', async () => {
    setTab(slot,'history');
    await queryHistory(slot,false);
  });
  $(`${slot}-more`).addEventListener('click', () => queryHistory(slot,true));
  for (const [direction, increment] of [['previous',-1],['next',1]]) {
    $(`${slot}-${direction}`).addEventListener('click', () => {
      ui.panels[slot].page += increment;
      ui.panels[slot].revision = -1;
      if (ui.state) renderActivity(slot, ui.state.traders[slot]);
    });
  }
  $(`${slot}-pause-log`).addEventListener('click', () => {
    const current = ui.panels[slot]; current.paused = !current.paused;
    current.frozenLogs = current.paused ? ui.logs.filter(row=>sourceMatch(row,slot)) : null;
    $(`${slot}-pause-log`).innerHTML = icon(current.paused ? 'play' : 'pause');
    $(`${slot}-pause-log`).title = current.paused ? 'Resume live log' : 'Pause live log';
    $(`${slot}-pause-log`).setAttribute('aria-label', $(`${slot}-pause-log`).title);
    renderSmallLogs();
  });
}

async function cancelOrder(slot, id) {
  try {await api('/api/cancel',{slot,order_id:id}); toast('Cancel sent · awaiting exchange response'); $(`${slot}-cancel-id`).value = '';}
  catch(error) {toast(error.message,'error');}
}
async function queryHistory(slot, more) {
  try {await api('/api/history',{slot,more});}
  catch(error) {toast(error.message,'error');}
}

$('configured-credentials').addEventListener('change', event => {
  $('password-label').hidden = event.target.checked;
  $('login-password').disabled = event.target.checked;
  $('login-password').required = !event.target.checked;
});
$('login-form').addEventListener('submit', async event => {
  event.preventDefault();
  const button = event.submitter; button.disabled = true;
  try {
    await api('/api/login',{slot:ui.loginSlot, username:$('login-user').value, configured:$('configured-credentials').checked, password:$('login-password').value});
    $('login-password').value = '';
    $('login-dialog').close();
    toast('Connecting account…');
  } catch(error) {$('login-error').textContent = error.message;}
  finally {button.disabled = false;}
});

function renderActivity(slot, trader) {
  const panel = ui.panels[slot], tab = panel.tab;
  $(`${slot}-order-count`).textContent = trader.orders.length;
  $(`${slot}-history-count`).textContent = trader.history.length;
  $(`${slot}-fill-count`).textContent = trader.fills.length;
  const activity = $(`${slot}-activity`);
  $(`${slot}-activity-caption`).textContent = tab === 'orders' ? (trader.status === 'Ready' ? 'Active orders' : 'Last known active orders') : tab === 'history' ? (trader.history_busy ? 'Querying committed trades…' : 'Committed trade history') : 'Live session fills';
  $(`${slot}-query`).disabled = trader.status !== 'Ready' || trader.history_busy;
  $(`${slot}-export`).hidden = tab !== 'history' || !trader.history.length;
  $(`${slot}-cancel-form`).hidden = tab !== 'orders';
  $(`${slot}-history-footer`).hidden = tab !== 'history';
  $(`${slot}-history-caption`).textContent = trader.history_error || `${number(trader.history.length)} loaded${trader.history_more ? ' · more available' : ''}`;
  $(`${slot}-more`).hidden = !trader.history_more;
  $(`${slot}-more`).disabled = trader.history_busy || trader.status !== 'Ready';
  const capacity = visibleCapacity(activity, '--activity-row-height', 46);
  const allRecords = tab === 'orders' ? [...trader.orders].reverse() : tab === 'history' ? trader.history : trader.fills;
  const page = pageSlice(allRecords, panel.page, capacity);
  panel.page = page.page;
  updatePager(`${slot}-previous`, `${slot}-next`, `${slot}-page-label`, page);
  if (panel.revision === trader.revision && panel.lastTab === tab && panel.lastCapacity === capacity && panel.lastPage === page.page) return;
  panel.revision = trader.revision; panel.lastTab = tab; panel.lastCapacity = capacity; panel.lastPage = page.page;
  let html;
  if (tab === 'orders') {
    html = page.items.length ? page.items.map(order => `<div class="order-row"><div class="order-row-main"><div class="order-row-title">${esc(order.symbol)} <span class="side-word ${className(order.side)}">${esc(order.side.toUpperCase())}</span></div><div class="order-row-sub" title="Order ID">${esc(order.id)}</div></div><div class="order-row-values">${number(order.remaining)} @ ${order.price}<small>${order.filled ? `${number(order.filled)} / ${number(order.original)} filled` : 'Resting'}</small></div><button class="icon-button cancel-small" data-cancel="${esc(order.id)}" title="Cancel order ${esc(order.id)}" aria-label="Cancel order ${esc(order.id)}" ${trader.status !== 'Ready' ? 'disabled' : ''}>${icon('close')}</button></div>`).join('') : empty('No active orders',trader.status === 'Ready' ? 'Resting limit orders will appear here.' : 'Log in to view your exchange orders.');
  } else {
    const records = page.items;
    html = records.length ? records.map(record => `<button class="history-row" data-trade-id="${esc(record.id)}" data-trade-type="${esc(record.type)}" title="View execution details"><div><div class="order-row-title">${esc(record.symbol)} <span class="side-word ${className(record.side)}">${esc(record.side.toUpperCase())}</span></div><span class="history-type">${esc(record.type)}</span> <span class="order-row-sub">${timeText(record.timestamp)}</span></div><div class="order-row-values">${number(record.quantity)} @ ${record.price}<small>View details ↗</small></div></button>`).join('') : empty(trader.history_busy && tab === 'history' ? 'Loading trade history' : tab === 'history' ? 'No trades loaded' : 'No fills yet',tab === 'history' ? 'Query trades to read persisted executions.' : 'New executions arrive here automatically.','activity');
  }
  activity.innerHTML = html;
}

function renderTraders() {
  for (const slot of ['alice','bob']) {
    const trader = ui.state.traders[slot];
    const live = trader.status === 'Ready';
    const active = ['Ready','Connecting','Synchronizing','Reconnecting'].includes(trader.status);
    $(`${slot}-status`).className = `account-state ${live?'live':active?'warning':''}`;
    $(`${slot}-status`).innerHTML = `<span class="dot"></span>${esc(trader.status)}`;
    $(`${slot}-status`).title = trader.error || trader.status;
    $(`${slot}-name`).textContent = trader.username[0]?.toUpperCase() + trader.username.slice(1);
    $(`${slot}-login`).textContent = active ? 'Log out' : 'Log in';
    updateTicket(slot);
    renderActivity(slot,trader);
  }
}

function depthSide(level, side, max, symbol) {
  if (!level) return `<button class="depth-side ${side}" disabled aria-label="Empty ${side} level">${side==='bid'?'<span class="depth-orders">—</span><span>—</span><span>—</span>':'<span>—</span><span>—</span><span class="depth-orders">—</span>'}</button>`;
  const total = level.quantity + level.implied;
  const background = `<i class="depth-fill" style="width:${max ? total/max*100 : 0}%"><b style="width:${total ? level.implied/total*100 : 0}%"></b></i>`;
  const price = `<span class="depth-price">${level.price}</span>`;
  const quantity = `<span class="depth-quantity">${number(level.quantity)}${level.implied?` <em class="implied-value">+${number(level.implied)}</em>`:''}</span>`;
  const orders = `<span class="depth-orders">${number(level.orders)}</span>`;
  const action = side==='bid'?'Sell':'Buy';
  return `<button class="depth-side ${side}" data-symbol="${esc(symbol)}" data-price="${level.price}" data-order-side="${action}" title="${action} ${esc(symbol)} at ${level.price} · Direct ${level.quantity}, implied ${level.implied}, orders ${level.orders}" aria-label="${action} ${esc(symbol)} at ${level.price}">${background}${side==='bid'?orders+quantity+price:price+quantity+orders}</button>`;
}

function bookCard(product) {
  const book = ui.state.market.books[String(product.id)] || {};
  const levels = book.levels || [];
  const bids = levels.filter(l=>l.side===1).sort((a,b)=>b.price-a.price);
  const asks = levels.filter(l=>l.side===2).sort((a,b)=>a.price-b.price);
  const max = Math.max(1,...levels.map(l=>l.quantity+l.implied));
  const formulas = {SP_AB:'A − B',SP_AC:'A − C',BF_ABC:'A − 2B + C'};
  const live = book.status === 'Live';
  return `<article class="book-card" data-book="${product.id}"><div class="book-title"><button class="book-select" data-symbol="${esc(product.symbol)}" title="Select ${esc(product.symbol)} for the active trading panel">${esc(product.symbol)}${formulas[product.symbol]?`<span class="book-formula">${formulas[product.symbol]}</span>`:''}</button><span class="book-updated ${live?'':'stale'}" title="${esc(book.timestamp || 'No snapshot yet')}">${live?timeText(book.timestamp):esc(book.status || 'Waiting')}</span></div><div class="depth-headers"><div class="depth-header"><span>#</span><span>QTY <span class="implied-value">+ I</span></span><span>BID</span></div><div class="depth-header ask"><span>ASK</span><span>QTY <span class="implied-value">+ I</span></span><span>#</span></div></div>${Array.from({length:5},(_,i)=>`<div class="depth-row">${depthSide(bids[i],'bid',max,product.symbol)}${depthSide(asks[i],'ask',max,product.symbol)}</div>`).join('')}${!levels.length?'<div class="empty-depth-note">'+(book.timestamp?'No resting liquidity':'Waiting for public snapshot')+'</div>':''}</article>`;
}

function fitBookGrid(container, rows) {
  // Keep the requested four/three rows; only tighten type when the window is short.
  const cardHeight = container.clientHeight / rows;
  const levelHeight = Math.max(0, (cardHeight - 37) / 5);
  const size = Math.max(9, Math.min(14, Math.floor(levelHeight * .84)));
  container.style.setProperty('--book-number-size', `${size}px`);
  container.style.setProperty('--book-title-size', `${Math.min(15, size + 2)}px`);
  container.style.setProperty('--book-meta-size', `${Math.max(8, Math.min(11, size - 1))}px`);
}

function renderMarkets(force=false) {
  if (!ui.state) return;
  fitBookGrid($('outright-books'), 4);
  fitBookGrid($('spread-books'), 3);
  // Freshness age changes on each poll; book DOM only needs actual content/status updates.
  const stable = JSON.stringify([ui.page,ui.search,ui.products.map(p=>p.symbol),Object.entries(ui.state.market.books).map(([id,b])=>[id,b.index,b.epoch,b.status,b.levels])]);
  if (!force && stable === ui.marketKey) return;
  ui.marketKey = stable;
  const search = ui.search.toUpperCase();
  const matches = p => p.symbol.toUpperCase().includes(search);
  const outright = ui.products.filter(p=>p.group==='Outrights' && matches(p));
  const spreads = ui.products.filter(p=>p.group==='Spreads' && matches(p));
  const outrightPage = pageSlice(outright, ui.page, OUTRIGHT_PAGE_SIZE);
  ui.page = outrightPage.page;
  $('outright-count').textContent = outright.length;
  $('spread-count').textContent = spreads.length;
  updatePager('previous-page', 'next-page', 'page-label', outrightPage);
  for (const [id,products] of [['outright-books',outrightPage.items],['spread-books',spreads]]) {
    const container = $(id);
    container.innerHTML = products.length ? products.map(bookCard).join('') : empty('No matching symbols','Try another symbol name.','search');
  }
  const live = Object.values(ui.state.market.books).filter(b=>b.status==='Live').length;
  $('market-summary').textContent = `${live} / ${ui.products.length} books live · ${number(ui.state.market.packets)} packets`;
}

document.querySelector('.markets-panel').addEventListener('click', event => {
  const target = event.target.closest('button[data-symbol]');
  if (!target) return;
  selectSymbol(ui.focused,target.dataset.symbol,target.dataset.price === undefined ? undefined : Number(target.dataset.price),target.dataset.orderSide);
});
$('previous-page').addEventListener('click',()=>{ui.page--; renderMarkets(true);});
$('next-page').addEventListener('click',()=>{ui.page++; renderMarkets(true);});
$('symbol-search').addEventListener('input',event=>{ui.search=event.target.value; ui.page=0; renderMarkets(true);});
$('refresh-market').addEventListener('click',async()=>{try{await api('/api/market/refresh',{});toast('TCP snapshot refresh requested');}catch(error){toast(error.message,'error');}});
document.addEventListener('keydown',event=>{if((event.metaKey||event.ctrlKey)&&event.key==='k'){event.preventDefault();$('symbol-search').focus();}});

function sourceMatch(row, source) {
  if (source==='all') return true;
  if (source==='publisher') return ['publisher','feed'].includes(row.source);
  if (source==='system') return !['publisher','feed','gateway','alice','bob'].includes(row.source);
  return row.source===source;
}
function logRow(row, full=false) {
  return `<div class="log-row ${className(row.level)}" title="${esc(row.time+' ['+row.source+'] '+row.text)}"><span class="log-time">${timeText(row.time)}</span>${full?`<span class="log-source">${esc(row.source)}</span>`:''}<span class="log-text">${esc(row.text)}</span></div>`;
}

function renderSmallLogs() {
  for (const source of ['publisher','gateway','alice','bob']) {
    const container = $(`${source}-log`), panel = ui.panels[source];
    const capacity = visibleCapacity(container, '--log-row-height', 18);
    const rows = (panel?.paused ? panel.frozenLogs || [] : ui.logs.filter(r=>sourceMatch(r,source))).slice(-capacity);
    const last = rows.at(-1)?.id || 0;
    const key = `${last}:${capacity}`;
    if (container.dataset.last===key) continue;
    container.innerHTML = rows.length ? rows.map(row=>logRow(row)).join('') : '<div class="log-empty">Waiting for activity…</div>';
    container.dataset.last = key;
  }
  if ($('log-dialog').open) renderJournal();
}

function filteredLogs() {
  const source=$('journal-source').value, level=$('journal-level').value, search=$('journal-search').value.toLowerCase();
  return (ui.journalPaused ? ui.journalFrozen : ui.logs).filter(row=>sourceMatch(row,source)&&(level==='all'||row.level===level)&&(!search||`${row.source} ${row.text}`.toLowerCase().includes(search)));
}
function renderJournal() {
  const output=$('journal-output'), rows=filteredLogs().reverse();
  const page=pageSlice(rows,ui.journalPage,visibleCapacity(output,'--journal-row-height',40));
  ui.journalPage=page.page;
  output.innerHTML=page.items.length?page.items.map(row=>logRow(row,true)).join(''):empty('No matching events','Change the source, level or search filter.','search');
  $('journal-count').textContent=rows.length;
  updatePager('journal-previous','journal-next','journal-page-label',page);
}
document.addEventListener('click',event=>{
  const expand=event.target.closest('.log-expand');
  if(expand){ui.journalPage=0;$('journal-source').value=expand.dataset.source;$('journal-search').value='';$('journal-level').value='all';$('log-dialog').showModal();renderJournal();}
  const close=event.target.closest('.close-dialog');
  if(close) close.closest('dialog').close();
});
document.querySelectorAll('dialog').forEach(dialog=>dialog.addEventListener('click',event=>{if(event.target===dialog){const r=dialog.getBoundingClientRect();if(event.clientX<r.left||event.clientX>r.right||event.clientY<r.top||event.clientY>r.bottom)dialog.close();}}));
for(const id of ['journal-source','journal-level','journal-search'])$(id).addEventListener(id==='journal-search'?'input':'change',()=>{ui.journalPage=0;renderJournal();});
function pauseJournal(paused) {
  ui.journalPaused=paused;ui.journalFrozen=paused?[...ui.logs]:null;
  $('journal-pause').innerHTML=icon(paused?'play':'pause')+(paused?'Resume':'Pause');
}
$('journal-pause').addEventListener('click',()=>{pauseJournal(!ui.journalPaused);ui.journalPage=0;renderJournal();});
$('journal-previous').addEventListener('click',()=>{ui.journalPage--;renderJournal();});
$('journal-next').addEventListener('click',()=>{if(!ui.journalPaused)pauseJournal(true);ui.journalPage++;renderJournal();});
$('journal-download').addEventListener('click',()=>{
  const text=filteredLogs().map(r=>`${r.time} [${r.source}] ${r.text}`).join('\n');
  const url=URL.createObjectURL(new Blob([text],{type:'text/plain'}));
  const link=document.createElement('a');link.href=url;link.download='marketlab-journal.log';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
});

function renderServices() {
  const s=ui.state.services, running=s.cluster!=='Offline';
  $('publisher-light').classList.toggle('live',s.publisher);
  $('gateway-light').classList.toggle('live',s.gateway);
  const sessions=Object.values(ui.state.traders).filter(t=>t.status==='Ready').length;
  $('gateway-status').textContent=s.gateway?`TCP available · ${sessions} active sessions`:'TCP unavailable';
  $('feed-transport').textContent=ui.state.market.transport;
  $('cluster-badge').textContent=s.cluster;
  $('cluster-badge').className=`badge ${className(s.cluster)}`;
  $('connection').innerHTML=`<span class="dot ${s.cluster==='Live'?'':'amber'}"></span>${s.cluster==='Live'?'Exchange live':s.cluster==='Paused'?'Exchange paused':'Engines offline'}`;
  const old=s.shards.map(row=>`${row.id}:${row.active}:${row.phase}:${row.connected}:${row.pid}`).join('|');
  if(old!==ui.systemKey || !$('shards').children.length){
    ui.systemKey=old;
    const ids=[...new Set(ui.products.map(p=>p.shard))].sort((a,b)=>a-b);
    $('shards').innerHTML=ids.map(id=>{
      const row=s.shards.find(r=>r.id===id);
      const status=row?.phase==='interrupted'?'paused':row?.connected?'live':'';
      return `<button class="shard ${status}" data-shard="${id}" title="Shard ${id}${row?` · ${esc(row.phase)} · PID ${row.pid}`:' · offline'}"><span class="dot"></span>${id}<span class="slot">${row?.active || '—'}</span></button>`;
    }).join('');
  }
  $('power-label').textContent=running?'Shutdown engines':s.data_dir?'Recover engines':'Start engines';
  $('power-button').dataset.action=running?'shutdown':'recover';
  $('power-button').classList.toggle('danger',running);
  $('power-button').disabled=Boolean(s.operation)||(running&&!s.owned);
  $('power-button').title=running&&!s.owned?'Attached services are owned by another terminal':'Gracefully stop or recover the matching cluster';
  $('restart-button').disabled=!running||Boolean(s.operation);
  $('fault-button').disabled=!s.owned||Boolean(s.operation);
  const status=$('operation-status');
  if(s.operation){status.className='operation-status busy';status.textContent=`${s.operation.action.toUpperCase()} · ${s.operation.target==='all'?'all shards':`shard ${s.operation.target}`} · ${Math.max(0,Math.floor(ui.state.time-s.operation.started))}s`;} 
  else if(s.error){status.className='operation-status error';status.textContent=s.error;}
  else if(s.cluster==='Paused'){status.className='operation-status busy';status.textContent=s.shards.some(row=>row.phase==='interrupted')?'Engine process interrupted · use Interrupt → Resume shard':'Exchange paused · inspect System log and Workspace details for recovery status';}
  else {status.className='operation-status';status.textContent=running?(s.owned?'Managed locally · data preserved across restarts':'Attached to existing services · planned restart available'):'Persistent state is kept. Recover engines to reconnect.';}
  $('footer-data').textContent=`${ui.products.length} symbols · ${s.shards.length || 8} shards · ${s.owned?'Managed':'Local'} workspace`;
}
async function serviceAction(action,target='all') {
  await api('/api/services',{action,target});
  toast(`${action[0].toUpperCase()+action.slice(1)} requested`);
}
$('power-button').addEventListener('click',async()=>{try{await serviceAction($('power-button').dataset.action);}catch(error){toast(error.message,'error');}});
$('restart-button').addEventListener('click',async()=>{try{await serviceAction('restart',$('restart-target').value);}catch(error){toast(error.message,'error');}});
$('shards').addEventListener('click',event=>{const target=event.target.closest('[data-shard]');if(target){$('restart-target').value=target.dataset.shard;$('fault-target').value=target.dataset.shard;}});
$('fault-button').addEventListener('click',()=>{$('fault-error').textContent='';$('crash-confirm').checked=false;$('crash-shard').disabled=true;$('fault-dialog').showModal();});
$('crash-confirm').addEventListener('change',event=>{$('crash-shard').disabled=!event.target.checked;});
for(const [id,action] of [['pause-shard','interrupt'],['resume-shard','resume'],['crash-shard','crash']])$(id).addEventListener('click',async()=>{
  try{await serviceAction(action,$('fault-target').value);$('fault-dialog').close();}catch(error){$('fault-error').textContent=error.message;}
});

function detailRow(label,value){return `<div class="detail-row"><dt>${esc(label)}</dt><dd>${esc(value ?? '—')}</dd></div>`;}
function tradeDetails(trade){
  const fields=[['Trade ID',trade.id],['Time (UTC)',trade.timestamp],['Symbol',trade.symbol],['Side',trade.side],['Quantity',trade.quantity],['Price',trade.price],['Execution',trade.type]];
  if(trade.order_id)fields.push(['Order ID',trade.order_id]);
  if(trade.buyer)fields.push(['Buyer',trade.buyer],['Seller',trade.seller],['Buy order',trade.buy_order_id],['Sell order',trade.sell_order_id]);
  let html='<dl>'+fields.map(([k,v])=>detailRow(k,v)).join('')+'</dl>';
  for(const leg of trade.legs||[])html+=`<h3 class="detail-label">${esc(leg.name)} leg</h3><dl>${detailRow('Order',leg.order_id)}${detailRow('Account',leg.user)}${detailRow('Price',leg.price)}</dl>`;
  $('trade-details').innerHTML=html;$('trade-dialog').showModal();
}
$('info-button').addEventListener('click',()=>{
  if(!ui.state)return;
  const s=ui.state.services;
  $('workspace-details').innerHTML='<dl>'+[['Connection','Local TCP / UDP'],['Configuration',ui.state.config_path],['Data directory',s.data_dir],['Engine logs',s.log_dir],['Market transport',ui.state.market.transport],['Public feed interval','1 second'],['Interface refresh','500 milliseconds'],['Group status',s.group]].map(([k,v])=>detailRow(k,v)).join('')+'</dl><p class="dialog-note">Orders and trades come from the running exchange. Closing a browser tab keeps services running. Stop the launch script with Ctrl-C for a graceful shutdown.</p>';
  $('info-dialog').showModal();
});

function initializeProducts(state){
  ui.products=state.products;
  const options=ui.products.map(p=>`<option value="${esc(p.symbol)}">${esc(p.symbol)}</option>`).join('');
  for(const slot of ['alice','bob']){$(`${slot}-symbol`).innerHTML=options;updateTicket(slot,true);}
  const shards=[...new Set(ui.products.map(p=>p.shard))].sort((a,b)=>a-b);
  $('restart-target').innerHTML='<option value="all">All shards</option>'+shards.map(id=>`<option value="${id}">Shard ${id}</option>`).join('');
  $('fault-target').innerHTML=shards.map(id=>`<option value="${id}">Shard ${id}</option>`).join('');
  $('fault-target').value=String(shards[0]);
}

async function poll(){
  const started=performance.now();
  try{
    const response=await fetch(`/api/state?after=${ui.cursor}`,{cache:'no-store',signal:AbortSignal.timeout(5000)});
    if(!response.ok)throw new Error(`Console returned ${response.status}`);
    let state=await response.json();
    if(ui.csrf && ui.csrf!==state.csrf){
      ui.cursor=0;ui.logs=[];ui.marketKey='';ui.systemKey='';ui.products=[];
      for(const p of Object.values(ui.panels))p.revision=-1;
      for(const source of ['publisher','gateway','alice','bob'])$(`${source}-log`).dataset.last='';
      const fresh=await fetch('/api/state',{cache:'no-store',signal:AbortSignal.timeout(5000)});
      if(!fresh.ok)throw new Error('Console is restarting');
      state=await fresh.json();
    }
    ui.csrf=state.csrf;ui.state=state;
    if(!ui.products.length)initializeProducts(state);
    const newRows=state.logs.filter(r=>r.id>ui.cursor);
    ui.logs.push(...newRows);ui.logs=ui.logs.slice(-4000);ui.cursor=state.log_sequence;
    ui.failures=0;$('connection-banner').hidden=true;
    renderServices();renderTraders();renderMarkets();renderSmallLogs();
    for(const row of newRows){if(['alice','bob'].includes(row.source)&&row.level==='error')toast(`${row.source}: ${row.text}`,'error');}
  }catch(error){
    ui.failures++;
    if(ui.failures>=2){$('connection-banner').hidden=false;$('connection').innerHTML='<span class="dot amber"></span>Console offline';for(const slot of ['alice','bob'])$(`${slot}-submit`).disabled=true;}
  }finally{setTimeout(poll,Math.max(50,500-(performance.now()-started)));}
}
setInterval(()=>{$('clock').textContent=new Date().toLocaleTimeString('en-GB',{hour12:false})},1000);
let layoutFrame;
const layoutObserver=new ResizeObserver(()=>{
  cancelAnimationFrame(layoutFrame);
  layoutFrame=requestAnimationFrame(()=>{
    if(!ui.state)return;
    renderMarkets();renderTraders();renderSmallLogs();
  });
});
for(const id of ['outright-books','spread-books','alice-activity','bob-activity','alice-log','bob-log','publisher-log','gateway-log','journal-output'])layoutObserver.observe($(id));
poll();
