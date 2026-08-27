/* Tracey Shop storefront — vanilla JS, no build step.
 *
 * Mirrors the five journeys the load generator simulates (browse by category,
 * search, view product, add to cart, checkout) so a human clicking through the
 * browser exercises exactly the same backend paths as the synthetic traffic.
 *
 * Three constraints from the backend that are easy to violate and produce 400s,
 * which would dirty the Causely baseline:
 *
 *   1. storefront-bff decodes POST bodies with DisallowUnknownFields. Every
 *      object posted below must carry ONLY the keys the Go structs declare
 *      (internal/domain/domain.go). Adding a stray field is a 400.
 *   2. There is no cart-creation endpoint — POST /api/cart/{id}/items upserts —
 *      so the client mints its own cart id and keeps it in localStorage.
 *   3. Checking out an empty cart is a client error, so #place-order stays
 *      disabled until the cart has something in it.
 *
 * Element ids are stable on purpose: analytics and RUM tools key their event and
 * funnel definitions off selectors, and renaming one silently breaks them.
 */
'use strict';

const CATEGORIES = [
  'electronics', 'apparel', 'home', 'outdoors',
  'books', 'toys', 'grocery', 'beauty',
];

const view = document.getElementById('view');
const banner = document.getElementById('error-banner');
const bannerMsg = document.getElementById('error-message');
const cartCount = document.getElementById('cart-count');

/* Product details are fetched on demand and remembered, because the cart only
 * stores product ids and quantities. */
const productCache = new Map();

/* ------------------------------------------------------------------ cart id */

/* Same shape as the load generator's domain.NewID("cart"). */
function newCartId() {
  const rnd = crypto.getRandomValues(new Uint8Array(8));
  return 'cart-' + Array.from(rnd, b => b.toString(16).padStart(2, '0')).join('');
}

function cartId() {
  let id = localStorage.getItem('tracey.cartId');
  if (!id) {
    id = newCartId();
    localStorage.setItem('tracey.cartId', id);
  }
  return id;
}

/* ---------------------------------------------------------------- utilities */

function money(m) {
  if (!m) return '$0.00';
  return '$' + (m.cents / 100).toFixed(2);
}

/* Deterministic colour per product id, so a product looks identical on every
 * reload and in every session replay. */
function hueOf(id) {
  let h = 0;
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) % 360;
  return h;
}

function thumb(product, extraClass) {
  const id = product.id || '';
  const h = hueOf(id);
  const initials = (product.name || id).split(' ')
    .filter(w => /[A-Za-z]/.test(w[0] || ''))
    .slice(0, 2).map(w => w[0].toUpperCase()).join('') || id.slice(-2);
  return `<div class="thumb ${extraClass || ''}"
      style="background:linear-gradient(135deg,hsl(${h} 62% 52%),hsl(${(h + 42) % 360} 58% 38%))"
      aria-hidden="true">${initials}</div>`;
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function showError(msg) {
  bannerMsg.textContent = msg;
  banner.hidden = false;
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function clearError() {
  banner.hidden = true;
  bannerMsg.textContent = '';
}

document.getElementById('error-dismiss').addEventListener('click', clearError);

function spinner(label) {
  view.innerHTML = `<div class="center"><span class="spinner"></span>
    <p class="muted">${esc(label || 'Loading…')}</p></div>`;
}

/* ------------------------------------------------------------------ the API */

/* One place where every request goes, so failures are reported consistently.
 * Throws an Error carrying the HTTP status and the backend's {"error": "..."}
 * message. */
async function api(method, path, body) {
  const opts = { method, headers: { 'Accept': 'application/json' } };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }

  const res = await fetch(path, opts);
  if (res.status === 204) return null;

  let payload = null;
  try {
    payload = await res.json();
  } catch {
    /* Non-JSON body (e.g. a proxy error page); fall through to the status. */
  }

  if (!res.ok) {
    const err = new Error((payload && payload.error) || `${method} ${path} failed (${res.status})`);
    err.status = res.status;
    throw err;
  }
  return payload;
}

async function getProduct(id) {
  if (productCache.has(id)) return productCache.get(id);
  const out = await api('GET', `/api/products/${encodeURIComponent(id)}`);
  const p = out && out.product;
  if (p) productCache.set(id, p);
  return p;
}

async function fetchCart() {
  const cart = await api('GET', `/api/cart/${encodeURIComponent(cartId())}`);
  return cart || { id: cartId(), items: [] };
}

function itemCount(cart) {
  return (cart.items || []).reduce((n, i) => n + (i.quantity || 0), 0);
}

async function refreshCartCount() {
  try {
    cartCount.textContent = String(itemCount(await fetchCart()));
  } catch {
    /* A cart read failure must not block rendering the page the user asked for. */
  }
}

/* ------------------------------------------------------------------ routing */

function navigate(path, replace) {
  if (replace) history.replaceState({}, '', path);
  else history.pushState({}, '', path);
  render();
}

/* Any element with data-nav becomes a pushState link, so the page navigates as a
 * route change rather than a full reload. */
document.addEventListener('click', ev => {
  const link = ev.target.closest('a[data-nav]');
  if (!link) return;
  if (ev.metaKey || ev.ctrlKey || ev.shiftKey || ev.button !== 0) return;
  ev.preventDefault();
  navigate(link.getAttribute('href'));
});

window.addEventListener('popstate', render);

/* --------------------------------------------------------------- categories */

function renderCategories(active) {
  const nav = document.getElementById('categories');
  const chip = (label, href, isActive) =>
    `<a class="chip" href="${href}" data-nav ${isActive ? 'aria-current="true"' : ''}>${esc(label)}</a>`;
  nav.innerHTML = chip('All', '/', !active)
    + CATEGORIES.map(c => chip(c, `/?category=${c}`, c === active)).join('');
}

/* -------------------------------------------------------------------- views */

async function viewCatalogue(params) {
  const category = params.get('category') || '';
  renderCategories(category);
  spinner('Loading products…');

  const qs = new URLSearchParams({ limit: '24' });
  if (category) qs.set('category', category);

  const out = await api('GET', `/api/products?${qs}`);
  const products = (out && out.products) || [];
  products.forEach(p => productCache.set(p.id, p));

  view.innerHTML = `
    <h1>${category ? esc(category[0].toUpperCase() + category.slice(1)) : 'Featured products'}</h1>
    ${products.length ? `<div class="grid" id="product-grid">${products.map(productCardHTML).join('')}</div>`
      : `<p class="muted">No products in this category.</p>`}`;
}

function productCardHTML(p) {
  return `
    <a class="product-card" data-nav data-product-id="${esc(p.id)}" href="/product/${esc(p.id)}">
      ${thumb(p)}
      <div class="card-body">
        <span class="card-name">${esc(p.name)}</span>
        <span class="card-cat">${esc(p.category)}</span>
        <span class="card-price">${money(p.price)}</span>
        ${p.available > 0 ? '' : '<span class="stock-out">Out of stock</span>'}
      </div>
    </a>`;
}

async function viewSearch(params) {
  const q = params.get('q') || '';
  renderCategories('');
  spinner(`Searching for “${q}”…`);

  const out = await api('GET', `/api/search?q=${encodeURIComponent(q)}&limit=24`);
  const products = (out && out.products) || [];
  products.forEach(p => productCache.set(p.id, p));

  view.innerHTML = `
    <h1>Results for “${esc(q)}”</h1>
    <p class="muted">${products.length} product${products.length === 1 ? '' : 's'}</p>
    ${products.length ? `<div class="grid" id="product-grid">${products.map(productCardHTML).join('')}</div>`
      : `<p class="muted">Nothing matched that search.</p>`}`;
}

async function viewProduct(id) {
  renderCategories('');
  spinner('Loading product…');

  const p = await getProduct(id);
  if (!p) {
    view.innerHTML = `<h1>Product not found</h1><a class="btn" href="/" data-nav>Back to the store</a>`;
    return;
  }

  view.innerHTML = `
    <div class="product">
      ${thumb(p)}
      <div class="product-meta">
        <div>
          <h1 style="margin-bottom:6px">${esc(p.name)}</h1>
          <span class="card-cat">${esc(p.category)} · ${esc(p.sku)}</span>
        </div>
        <div class="price-lg">${money(p.price)}</div>
        <p class="muted">${p.available > 0
          ? `${p.available} in stock, ships in 3–5 business days.`
          : `<span class="stock-out">Currently out of stock.</span>`}</p>
        <div class="qty">
          <label for="quantity">Quantity</label>
          <input id="quantity" type="number" min="1" max="5" value="1">
        </div>
        <div class="btn-row">
          <button id="add-to-cart" class="btn" type="button"
            data-product-id="${esc(p.id)}" ${p.available > 0 ? '' : 'disabled'}>Add to cart</button>
          <a class="btn btn-secondary" href="/cart" data-nav>Go to cart</a>
        </div>
      </div>
    </div>`;

  document.getElementById('add-to-cart').addEventListener('click', async ev => {
    const btn = ev.currentTarget;
    const qty = Math.max(1, parseInt(document.getElementById('quantity').value, 10) || 1);
    btn.disabled = true;
    btn.textContent = 'Adding…';
    try {
      /* Exactly domain.AddToCartRequest — no extra keys. */
      const cart = await api('POST', `/api/cart/${encodeURIComponent(cartId())}/items`,
        { productId: p.id, quantity: qty });
      cartCount.textContent = String(itemCount(cart || { items: [] }));
      clearError();
      btn.textContent = 'Added ✓';
      setTimeout(() => { btn.textContent = 'Add to cart'; btn.disabled = false; }, 900);
    } catch (err) {
      showError(`We couldn't add that to your cart. ${err.message}`);
      btn.textContent = 'Add to cart';
      btn.disabled = false;
    }
  });
}

async function viewCart() {
  renderCategories('');
  spinner('Loading your cart…');

  const cart = await fetchCart();
  const items = cart.items || [];
  cartCount.textContent = String(itemCount(cart));

  if (!items.length) {
    view.innerHTML = `
      <h1>Your cart</h1>
      <div class="panel"><p class="muted">Your cart is empty.</p>
      <a class="btn" href="/" data-nav style="margin-top:12px;display:inline-block">Browse the store</a></div>`;
    return;
  }

  /* The cart holds only ids and quantities, so fetch the details to render it. */
  const detailed = await Promise.all(items.map(async i => ({
    item: i,
    product: await getProduct(i.productId).catch(() => null),
  })));

  const subtotal = detailed.reduce((sum, d) =>
    sum + ((d.product && d.product.price ? d.product.price.cents : 0) * d.item.quantity), 0);

  view.innerHTML = `
    <h1>Your cart</h1>
    <div class="two-col">
      <div class="panel" id="cart-items">
        ${detailed.map(d => `
          <div class="cart-row" data-product-id="${esc(d.item.productId)}">
            ${d.product ? thumb(d.product) : ''}
            <div class="grow">
              <div style="font-weight:600">${esc(d.product ? d.product.name : d.item.productId)}</div>
              <div class="card-cat">Qty ${d.item.quantity}</div>
            </div>
            <div style="font-weight:700">${money({
              cents: (d.product && d.product.price ? d.product.price.cents : 0) * d.item.quantity })}</div>
          </div>`).join('')}
        <div class="btn-row" style="margin-top:16px">
          <button id="clear-cart" class="btn btn-secondary" type="button">Empty cart</button>
        </div>
      </div>

      <div class="panel">
        <h2>Summary</h2>
        <div class="totals">
          <div><span class="muted">Subtotal</span><span>${money({ cents: subtotal })}</span></div>
          <div><span class="muted">Shipping &amp; tax</span><span class="muted">calculated at checkout</span></div>
          <div class="grand"><span>Estimated total</span><span>${money({ cents: subtotal })}</span></div>
        </div>
        <a id="go-to-checkout" class="btn" href="/checkout" data-nav
           style="margin-top:16px;display:inline-block">Proceed to checkout</a>
      </div>
    </div>`;

  document.getElementById('clear-cart').addEventListener('click', async ev => {
    ev.currentTarget.disabled = true;
    try {
      await api('POST', `/api/cart/${encodeURIComponent(cartId())}/clear`);
      clearError();
      render();
    } catch (err) {
      showError(`We couldn't empty your cart. ${err.message}`);
      ev.currentTarget.disabled = false;
    }
  });
}

/* The checkout form. Values mirror what the load generator sends, so browser and
 * synthetic orders look alike downstream. */
function checkoutFormHTML() {
  const n = Math.floor(Math.random() * 500);
  return `
    <div class="field">
      <label for="email">Email</label>
      <input id="email" type="email" value="shopper${String(n).padStart(4, '0')}@example.com">
    </div>
    <div class="field">
      <label for="street">Street address</label>
      <input id="street" value="${100 + Math.floor(Math.random() * 900)} Market St">
    </div>
    <div class="field-row">
      <div class="field"><label for="city">City</label><input id="city" value="Springfield"></div>
      <div class="field"><label for="region">State</label><input id="region" value="CA"></div>
    </div>
    <div class="field-row">
      <div class="field"><label for="postalCode">ZIP</label>
        <input id="postalCode" value="9${String(Math.floor(Math.random() * 10000)).padStart(4, '0')}"></div>
      <div class="field"><label for="country">Country</label>
        <select id="country"><option>US</option><option>CA</option><option>GB</option><option>DE</option></select></div>
    </div>
    <div class="field-row">
      <div class="field"><label for="cardLastFour">Card (last 4)</label>
        <input id="cardLastFour" maxlength="4" value="4242"></div>
      <div class="field"><label for="customerTier">Membership</label>
        <select id="customerTier"><option>standard</option><option selected>gold</option><option>platinum</option></select></div>
    </div>`;
}

async function viewCheckout() {
  renderCategories('');
  spinner('Preparing checkout…');

  const cart = await fetchCart();
  const items = cart.items || [];
  cartCount.textContent = String(itemCount(cart));

  view.innerHTML = `
    <h1>Checkout</h1>
    <div class="two-col">
      <div class="panel">
        <h2>Shipping &amp; payment</h2>
        ${checkoutFormHTML()}
      </div>
      <div class="panel">
        <h2>Order summary</h2>
        <p class="muted">${itemCount(cart)} item${itemCount(cart) === 1 ? '' : 's'} in your cart</p>
        <div class="btn-row" style="margin-top:16px">
          <button id="place-order" class="btn" type="button" ${items.length ? '' : 'disabled'}>
            Place order</button>
        </div>
        ${items.length ? '' : `<p class="muted" style="margin-top:10px">
          Your cart is empty — <a href="/" data-nav>add something first</a>.</p>`}
      </div>
    </div>`;

  const btn = document.getElementById('place-order');
  if (items.length) btn.addEventListener('click', () => placeOrder(btn));
}

/* placeOrder is the moment the demo hinges on. When payment-gw is failing this is
 * where the user sees it, and the retry button is what turns one annoyed click
 * into the repeat-attempt pattern a frustrated shopper actually produces. */
async function placeOrder(btn) {
  const val = id => {
    const el = document.getElementById(id);
    return el ? el.value : '';
  };

  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Placing order…';
  clearError();

  /* Exactly domain.CheckoutRequest — the backend rejects unknown fields. */
  const body = {
    cartId: cartId(),
    customerId: 'web-' + cartId().slice(-6),
    customerTier: val('customerTier') || 'standard',
    email: val('email'),
    address: {
      street: val('street'),
      city: val('city'),
      region: val('region'),
      postalCode: val('postalCode'),
      country: val('country') || 'US',
    },
    cardLastFour: val('cardLastFour'),
    cardBrand: 'visa',
  };

  try {
    const order = await api('POST', '/api/checkout', body);

    /* checkout-api clears the cart server-side on success, so the badge has to be
     * re-read rather than assumed. */
    localStorage.removeItem('tracey.cartId');
    await refreshCartCount();
    navigate(`/order/${encodeURIComponent(order.orderId)}`);
  } catch (err) {
    /* A deliberately customer-facing message: this is what the demo audience
     * sees, and what they watch the shopper react to. */
    showError("We couldn't process your payment. Please try again.");

    /* Deliberately never render err.message here. Backend error strings travel
     * up the whole gRPC chain, so this is where internal detail would leak onto
     * the screen in front of the audience. A real storefront would not show it
     * either. It goes to the console for whoever is debugging. */
    console.error('checkout failed:', err.message);

    btn.disabled = false;
    btn.textContent = 'Place order';
    if (!document.getElementById('retry-order')) {
      const retry = document.createElement('button');
      retry.id = 'retry-order';
      retry.type = 'button';
      retry.className = 'btn btn-secondary';
      retry.textContent = 'Retry payment';
      retry.addEventListener('click', () => placeOrder(btn));
      btn.parentElement.appendChild(retry);
    }
  }
}

async function viewOrder(id) {
  renderCategories('');
  spinner('Loading your order…');

  const order = await api('GET', `/api/orders/${encodeURIComponent(id)}`);
  const items = (order && order.items) || [];

  view.innerHTML = `
    <div class="order-ok">Thank you — your order is confirmed.</div>
    <div class="panel">
      <h2>Order ${esc(order.orderId)}</h2>
      <dl class="kv">
        <dt>Status</dt><dd>${esc(order.status)}</dd>
        <dt>Total</dt><dd>${money(order.total)}</dd>
        <dt>Items</dt><dd>${items.reduce((n, i) => n + i.quantity, 0)}</dd>
      </dl>
      <div class="btn-row" style="margin-top:18px">
        <a class="btn" href="/" data-nav>Continue shopping</a>
      </div>
    </div>`;
}

/* ------------------------------------------------------------------- render */

async function render() {
  const url = new URL(window.location.href);
  const path = url.pathname;

  try {
    if (path === '/' || path === '/index.html') return await viewCatalogue(url.searchParams);
    if (path === '/search') return await viewSearch(url.searchParams);
    if (path.startsWith('/product/')) return await viewProduct(decodeURIComponent(path.slice('/product/'.length)));
    if (path === '/cart') return await viewCart();
    if (path === '/checkout') return await viewCheckout();
    if (path.startsWith('/order/')) return await viewOrder(decodeURIComponent(path.slice('/order/'.length)));

    view.innerHTML = `<h1>Page not found</h1><a class="btn" href="/" data-nav>Back to the store</a>`;
  } catch (err) {
    showError(err.message || 'Something went wrong loading this page.');
    view.innerHTML = `<div class="center"><p class="muted">This page couldn't be loaded.</p>
      <button class="btn" type="button" onclick="location.reload()">Reload</button></div>`;
  }
}

document.getElementById('search-form').addEventListener('submit', ev => {
  ev.preventDefault();
  const q = document.getElementById('search').value.trim();
  /* The backend requires a non-empty q, so an empty search goes to the
   * catalogue rather than manufacturing a 400. */
  navigate(q ? `/search?q=${encodeURIComponent(q)}` : '/');
});

refreshCartCount();
render();
