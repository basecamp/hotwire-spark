var HotwireSpark = (function () {
  'use strict';

  var adapters = {
    logger: typeof console !== "undefined" ? console : undefined,
    WebSocket: typeof WebSocket !== "undefined" ? WebSocket : undefined
  };

  var logger = {
    log(...messages) {
      if (this.enabled) {
        messages.push(Date.now());
        adapters.logger.log("[ActionCable]", ...messages);
      }
    }
  };

  const now = () => (new Date).getTime();

  const secondsSince = time => (now() - time) / 1e3;

  class ConnectionMonitor {
    constructor(connection) {
      this.visibilityDidChange = this.visibilityDidChange.bind(this);
      this.connection = connection;
      this.reconnectAttempts = 0;
    }
    start() {
      if (!this.isRunning()) {
        this.startedAt = now();
        delete this.stoppedAt;
        this.startPolling();
        addEventListener("visibilitychange", this.visibilityDidChange);
        logger.log(`ConnectionMonitor started. stale threshold = ${this.constructor.staleThreshold} s`);
      }
    }
    stop() {
      if (this.isRunning()) {
        this.stoppedAt = now();
        this.stopPolling();
        removeEventListener("visibilitychange", this.visibilityDidChange);
        logger.log("ConnectionMonitor stopped");
      }
    }
    isRunning() {
      return this.startedAt && !this.stoppedAt;
    }
    recordMessage() {
      this.pingedAt = now();
    }
    recordConnect() {
      this.reconnectAttempts = 0;
      delete this.disconnectedAt;
      logger.log("ConnectionMonitor recorded connect");
    }
    recordDisconnect() {
      this.disconnectedAt = now();
      logger.log("ConnectionMonitor recorded disconnect");
    }
    startPolling() {
      this.stopPolling();
      this.poll();
    }
    stopPolling() {
      clearTimeout(this.pollTimeout);
    }
    poll() {
      this.pollTimeout = setTimeout((() => {
        this.reconnectIfStale();
        this.poll();
      }), this.getPollInterval());
    }
    getPollInterval() {
      const {staleThreshold: staleThreshold, reconnectionBackoffRate: reconnectionBackoffRate} = this.constructor;
      const backoff = Math.pow(1 + reconnectionBackoffRate, Math.min(this.reconnectAttempts, 10));
      const jitterMax = this.reconnectAttempts === 0 ? 1 : reconnectionBackoffRate;
      const jitter = jitterMax * Math.random();
      return staleThreshold * 1e3 * backoff * (1 + jitter);
    }
    reconnectIfStale() {
      if (this.connectionIsStale()) {
        logger.log(`ConnectionMonitor detected stale connection. reconnectAttempts = ${this.reconnectAttempts}, time stale = ${secondsSince(this.refreshedAt)} s, stale threshold = ${this.constructor.staleThreshold} s`);
        this.reconnectAttempts++;
        if (this.disconnectedRecently()) {
          logger.log(`ConnectionMonitor skipping reopening recent disconnect. time disconnected = ${secondsSince(this.disconnectedAt)} s`);
        } else {
          logger.log("ConnectionMonitor reopening");
          this.connection.reopen();
        }
      }
    }
    get refreshedAt() {
      return this.pingedAt ? this.pingedAt : this.startedAt;
    }
    connectionIsStale() {
      return secondsSince(this.refreshedAt) > this.constructor.staleThreshold;
    }
    disconnectedRecently() {
      return this.disconnectedAt && secondsSince(this.disconnectedAt) < this.constructor.staleThreshold;
    }
    visibilityDidChange() {
      if (document.visibilityState === "visible") {
        setTimeout((() => {
          if (this.connectionIsStale() || !this.connection.isOpen()) {
            logger.log(`ConnectionMonitor reopening stale connection on visibilitychange. visibilityState = ${document.visibilityState}`);
            this.connection.reopen();
          }
        }), 200);
      }
    }
  }

  ConnectionMonitor.staleThreshold = 6;

  ConnectionMonitor.reconnectionBackoffRate = .15;

  var INTERNAL = {
    message_types: {
      welcome: "welcome",
      disconnect: "disconnect",
      ping: "ping",
      confirmation: "confirm_subscription",
      rejection: "reject_subscription"
    },
    disconnect_reasons: {
      unauthorized: "unauthorized",
      invalid_request: "invalid_request",
      server_restart: "server_restart",
      remote: "remote"
    },
    default_mount_path: "/cable",
    protocols: [ "actioncable-v1-json", "actioncable-unsupported" ]
  };

  const {message_types: message_types, protocols: protocols} = INTERNAL;

  const supportedProtocols = protocols.slice(0, protocols.length - 1);

  const indexOf = [].indexOf;

  class Connection {
    constructor(consumer) {
      this.open = this.open.bind(this);
      this.consumer = consumer;
      this.subscriptions = this.consumer.subscriptions;
      this.monitor = new ConnectionMonitor(this);
      this.disconnected = true;
    }
    send(data) {
      if (this.isOpen()) {
        this.webSocket.send(JSON.stringify(data));
        return true;
      } else {
        return false;
      }
    }
    open() {
      if (this.isActive()) {
        logger.log(`Attempted to open WebSocket, but existing socket is ${this.getState()}`);
        return false;
      } else {
        const socketProtocols = [ ...protocols, ...this.consumer.subprotocols || [] ];
        logger.log(`Opening WebSocket, current state is ${this.getState()}, subprotocols: ${socketProtocols}`);
        if (this.webSocket) {
          this.uninstallEventHandlers();
        }
        this.webSocket = new adapters.WebSocket(this.consumer.url, socketProtocols);
        this.installEventHandlers();
        this.monitor.start();
        return true;
      }
    }
    close({allowReconnect: allowReconnect} = {
      allowReconnect: true
    }) {
      if (!allowReconnect) {
        this.monitor.stop();
      }
      if (this.isOpen()) {
        return this.webSocket.close();
      }
    }
    reopen() {
      logger.log(`Reopening WebSocket, current state is ${this.getState()}`);
      if (this.isActive()) {
        try {
          return this.close();
        } catch (error) {
          logger.log("Failed to reopen WebSocket", error);
        } finally {
          logger.log(`Reopening WebSocket in ${this.constructor.reopenDelay}ms`);
          setTimeout(this.open, this.constructor.reopenDelay);
        }
      } else {
        return this.open();
      }
    }
    getProtocol() {
      if (this.webSocket) {
        return this.webSocket.protocol;
      }
    }
    isOpen() {
      return this.isState("open");
    }
    isActive() {
      return this.isState("open", "connecting");
    }
    triedToReconnect() {
      return this.monitor.reconnectAttempts > 0;
    }
    isProtocolSupported() {
      return indexOf.call(supportedProtocols, this.getProtocol()) >= 0;
    }
    isState(...states) {
      return indexOf.call(states, this.getState()) >= 0;
    }
    getState() {
      if (this.webSocket) {
        for (let state in adapters.WebSocket) {
          if (adapters.WebSocket[state] === this.webSocket.readyState) {
            return state.toLowerCase();
          }
        }
      }
      return null;
    }
    installEventHandlers() {
      for (let eventName in this.events) {
        const handler = this.events[eventName].bind(this);
        this.webSocket[`on${eventName}`] = handler;
      }
    }
    uninstallEventHandlers() {
      for (let eventName in this.events) {
        this.webSocket[`on${eventName}`] = function() {};
      }
    }
  }

  Connection.reopenDelay = 500;

  Connection.prototype.events = {
    message(event) {
      if (!this.isProtocolSupported()) {
        return;
      }
      const {identifier: identifier, message: message, reason: reason, reconnect: reconnect, type: type} = JSON.parse(event.data);
      this.monitor.recordMessage();
      switch (type) {
       case message_types.welcome:
        if (this.triedToReconnect()) {
          this.reconnectAttempted = true;
        }
        this.monitor.recordConnect();
        return this.subscriptions.reload();

       case message_types.disconnect:
        logger.log(`Disconnecting. Reason: ${reason}`);
        return this.close({
          allowReconnect: reconnect
        });

       case message_types.ping:
        return null;

       case message_types.confirmation:
        this.subscriptions.confirmSubscription(identifier);
        if (this.reconnectAttempted) {
          this.reconnectAttempted = false;
          return this.subscriptions.notify(identifier, "connected", {
            reconnected: true
          });
        } else {
          return this.subscriptions.notify(identifier, "connected", {
            reconnected: false
          });
        }

       case message_types.rejection:
        return this.subscriptions.reject(identifier);

       default:
        return this.subscriptions.notify(identifier, "received", message);
      }
    },
    open() {
      logger.log(`WebSocket onopen event, using '${this.getProtocol()}' subprotocol`);
      this.disconnected = false;
      if (!this.isProtocolSupported()) {
        logger.log("Protocol is unsupported. Stopping monitor and disconnecting.");
        return this.close({
          allowReconnect: false
        });
      }
    },
    close(event) {
      logger.log("WebSocket onclose event");
      if (this.disconnected) {
        return;
      }
      this.disconnected = true;
      this.monitor.recordDisconnect();
      return this.subscriptions.notifyAll("disconnected", {
        willAttemptReconnect: this.monitor.isRunning()
      });
    },
    error() {
      logger.log("WebSocket onerror event");
    }
  };

  const extend = function(object, properties) {
    if (properties != null) {
      for (let key in properties) {
        const value = properties[key];
        object[key] = value;
      }
    }
    return object;
  };

  class Subscription {
    constructor(consumer, params = {}, mixin) {
      this.consumer = consumer;
      this.identifier = JSON.stringify(params);
      extend(this, mixin);
    }
    perform(action, data = {}) {
      data.action = action;
      return this.send(data);
    }
    send(data) {
      return this.consumer.send({
        command: "message",
        identifier: this.identifier,
        data: JSON.stringify(data)
      });
    }
    unsubscribe() {
      return this.consumer.subscriptions.remove(this);
    }
  }

  class SubscriptionGuarantor {
    constructor(subscriptions) {
      this.subscriptions = subscriptions;
      this.pendingSubscriptions = [];
    }
    guarantee(subscription) {
      if (this.pendingSubscriptions.indexOf(subscription) == -1) {
        logger.log(`SubscriptionGuarantor guaranteeing ${subscription.identifier}`);
        this.pendingSubscriptions.push(subscription);
      } else {
        logger.log(`SubscriptionGuarantor already guaranteeing ${subscription.identifier}`);
      }
      this.startGuaranteeing();
    }
    forget(subscription) {
      logger.log(`SubscriptionGuarantor forgetting ${subscription.identifier}`);
      this.pendingSubscriptions = this.pendingSubscriptions.filter((s => s !== subscription));
    }
    startGuaranteeing() {
      this.stopGuaranteeing();
      this.retrySubscribing();
    }
    stopGuaranteeing() {
      clearTimeout(this.retryTimeout);
    }
    retrySubscribing() {
      this.retryTimeout = setTimeout((() => {
        if (this.subscriptions && typeof this.subscriptions.subscribe === "function") {
          this.pendingSubscriptions.map((subscription => {
            logger.log(`SubscriptionGuarantor resubscribing ${subscription.identifier}`);
            this.subscriptions.subscribe(subscription);
          }));
        }
      }), 500);
    }
  }

  class Subscriptions {
    constructor(consumer) {
      this.consumer = consumer;
      this.guarantor = new SubscriptionGuarantor(this);
      this.subscriptions = [];
    }
    create(channelName, mixin) {
      const channel = channelName;
      const params = typeof channel === "object" ? channel : {
        channel: channel
      };
      const subscription = new Subscription(this.consumer, params, mixin);
      return this.add(subscription);
    }
    add(subscription) {
      this.subscriptions.push(subscription);
      this.consumer.ensureActiveConnection();
      this.notify(subscription, "initialized");
      this.subscribe(subscription);
      return subscription;
    }
    remove(subscription) {
      this.forget(subscription);
      if (!this.findAll(subscription.identifier).length) {
        this.sendCommand(subscription, "unsubscribe");
      }
      return subscription;
    }
    reject(identifier) {
      return this.findAll(identifier).map((subscription => {
        this.forget(subscription);
        this.notify(subscription, "rejected");
        return subscription;
      }));
    }
    forget(subscription) {
      this.guarantor.forget(subscription);
      this.subscriptions = this.subscriptions.filter((s => s !== subscription));
      return subscription;
    }
    findAll(identifier) {
      return this.subscriptions.filter((s => s.identifier === identifier));
    }
    reload() {
      return this.subscriptions.map((subscription => this.subscribe(subscription)));
    }
    notifyAll(callbackName, ...args) {
      return this.subscriptions.map((subscription => this.notify(subscription, callbackName, ...args)));
    }
    notify(subscription, callbackName, ...args) {
      let subscriptions;
      if (typeof subscription === "string") {
        subscriptions = this.findAll(subscription);
      } else {
        subscriptions = [ subscription ];
      }
      return subscriptions.map((subscription => typeof subscription[callbackName] === "function" ? subscription[callbackName](...args) : undefined));
    }
    subscribe(subscription) {
      if (this.sendCommand(subscription, "subscribe")) {
        this.guarantor.guarantee(subscription);
      }
    }
    confirmSubscription(identifier) {
      logger.log(`Subscription confirmed ${identifier}`);
      this.findAll(identifier).map((subscription => this.guarantor.forget(subscription)));
    }
    sendCommand(subscription, command) {
      const {identifier: identifier} = subscription;
      return this.consumer.send({
        command: command,
        identifier: identifier
      });
    }
  }

  class Consumer {
    constructor(url) {
      this._url = url;
      this.subscriptions = new Subscriptions(this);
      this.connection = new Connection(this);
      this.subprotocols = [];
    }
    get url() {
      return createWebSocketURL(this._url);
    }
    send(data) {
      return this.connection.send(data);
    }
    connect() {
      return this.connection.open();
    }
    disconnect() {
      return this.connection.close({
        allowReconnect: false
      });
    }
    ensureActiveConnection() {
      if (!this.connection.isActive()) {
        return this.connection.open();
      }
    }
    addSubProtocol(subprotocol) {
      this.subprotocols = [ ...this.subprotocols, subprotocol ];
    }
  }

  function createWebSocketURL(url) {
    if (typeof url === "function") {
      url = url();
    }
    if (url && !/^wss?:/i.test(url)) {
      const a = document.createElement("a");
      a.href = url;
      a.href = a.href;
      a.protocol = a.protocol.replace("http", "ws");
      return a.href;
    } else {
      return url;
    }
  }

  function createConsumer(url = getConfig("url") || INTERNAL.default_mount_path) {
    return new Consumer(url);
  }

  function getConfig(name) {
    const element = document.head.querySelector(`meta[name='action-cable-${name}']`);
    if (element) {
      return element.getAttribute("content");
    }
  }

  var consumer = createConsumer();

  function assetNameFromPath(path) {
    return path.split("/").pop().split(".")[0];
  }
  function pathWithoutAssetDigest(path) {
    return path.replace(/-[a-z0-9]+\.(\w+)(\?.*)?$/, ".$1");
  }
  function urlWithParams(urlString, params) {
    const url = new URL(urlString, window.location.origin);
    Object.entries(params).forEach(_ref => {
      let [key, value] = _ref;
      url.searchParams.set(key, value);
    });
    return url.toString();
  }
  function cacheBustedUrl(urlString) {
    return urlWithParams(urlString, {
      reload: Date.now()
    });
  }
  async function reloadHtmlDocument() {
    let currentUrl = cacheBustedUrl(urlWithParams(window.location.href, {
      hotwire_spark: "true"
    }));
    const response = await fetch(currentUrl, {
      headers: {
        "Accept": "text/html"
      }
    });
    if (!response.ok) {
      throw new Error(`${response.status} when fetching ${currentUrl}`);
    }
    const fetchedHTML = await response.text();
    const parser = new DOMParser();
    return parser.parseFromString(fetchedHTML, "text/html");
  }
  function getConfigurationProperty(name) {
    return document.querySelector(`meta[name="hotwire-spark:${name}"]`)?.content;
  }

  // base IIFE to define idiomorph
  var Idiomorph = (function () {

          //=============================================================================
          // AND NOW IT BEGINS...
          //=============================================================================
          let EMPTY_SET = new Set();

          // default configuration values, updatable by users now
          let defaults = {
              morphStyle: "outerHTML",
              callbacks : {
                  beforeNodeAdded: noOp,
                  afterNodeAdded: noOp,
                  beforeNodeMorphed: noOp,
                  afterNodeMorphed: noOp,
                  beforeNodeRemoved: noOp,
                  afterNodeRemoved: noOp,
                  beforeAttributeUpdated: noOp,

              },
              head: {
                  style: 'merge',
                  shouldPreserve: function (elt) {
                      return elt.getAttribute("im-preserve") === "true";
                  },
                  shouldReAppend: function (elt) {
                      return elt.getAttribute("im-re-append") === "true";
                  },
                  shouldRemove: noOp,
                  afterHeadMorphed: noOp,
              }
          };

          //=============================================================================
          // Core Morphing Algorithm - morph, morphNormalizedContent, morphOldNodeTo, morphChildren
          //=============================================================================
          function morph(oldNode, newContent, config = {}) {

              if (oldNode instanceof Document) {
                  oldNode = oldNode.documentElement;
              }

              if (typeof newContent === 'string') {
                  newContent = parseContent(newContent);
              }

              let normalizedContent = normalizeContent(newContent);

              let ctx = createMorphContext(oldNode, normalizedContent, config);

              return morphNormalizedContent(oldNode, normalizedContent, ctx);
          }

          function morphNormalizedContent(oldNode, normalizedNewContent, ctx) {
              if (ctx.head.block) {
                  let oldHead = oldNode.querySelector('head');
                  let newHead = normalizedNewContent.querySelector('head');
                  if (oldHead && newHead) {
                      let promises = handleHeadElement(newHead, oldHead, ctx);
                      // when head promises resolve, call morph again, ignoring the head tag
                      Promise.all(promises).then(function () {
                          morphNormalizedContent(oldNode, normalizedNewContent, Object.assign(ctx, {
                              head: {
                                  block: false,
                                  ignore: true
                              }
                          }));
                      });
                      return;
                  }
              }

              if (ctx.morphStyle === "innerHTML") {

                  // innerHTML, so we are only updating the children
                  morphChildren(normalizedNewContent, oldNode, ctx);
                  return oldNode.children;

              } else if (ctx.morphStyle === "outerHTML" || ctx.morphStyle == null) {
                  // otherwise find the best element match in the new content, morph that, and merge its siblings
                  // into either side of the best match
                  let bestMatch = findBestNodeMatch(normalizedNewContent, oldNode, ctx);

                  // stash the siblings that will need to be inserted on either side of the best match
                  let previousSibling = bestMatch?.previousSibling;
                  let nextSibling = bestMatch?.nextSibling;

                  // morph it
                  let morphedNode = morphOldNodeTo(oldNode, bestMatch, ctx);

                  if (bestMatch) {
                      // if there was a best match, merge the siblings in too and return the
                      // whole bunch
                      return insertSiblings(previousSibling, morphedNode, nextSibling);
                  } else {
                      // otherwise nothing was added to the DOM
                      return []
                  }
              } else {
                  throw "Do not understand how to morph style " + ctx.morphStyle;
              }
          }


          /**
           * @param possibleActiveElement
           * @param ctx
           * @returns {boolean}
           */
          function ignoreValueOfActiveElement(possibleActiveElement, ctx) {
              return ctx.ignoreActiveValue && possibleActiveElement === document.activeElement;
          }

          /**
           * @param oldNode root node to merge content into
           * @param newContent new content to merge
           * @param ctx the merge context
           * @returns {Element} the element that ended up in the DOM
           */
          function morphOldNodeTo(oldNode, newContent, ctx) {
              if (ctx.ignoreActive && oldNode === document.activeElement) ; else if (newContent == null) {
                  if (ctx.callbacks.beforeNodeRemoved(oldNode) === false) return oldNode;

                  oldNode.remove();
                  ctx.callbacks.afterNodeRemoved(oldNode);
                  return null;
              } else if (!isSoftMatch(oldNode, newContent)) {
                  if (ctx.callbacks.beforeNodeRemoved(oldNode) === false) return oldNode;
                  if (ctx.callbacks.beforeNodeAdded(newContent) === false) return oldNode;

                  oldNode.parentElement.replaceChild(newContent, oldNode);
                  ctx.callbacks.afterNodeAdded(newContent);
                  ctx.callbacks.afterNodeRemoved(oldNode);
                  return newContent;
              } else {
                  if (ctx.callbacks.beforeNodeMorphed(oldNode, newContent) === false) return oldNode;

                  if (oldNode instanceof HTMLHeadElement && ctx.head.ignore) ; else if (oldNode instanceof HTMLHeadElement && ctx.head.style !== "morph") {
                      handleHeadElement(newContent, oldNode, ctx);
                  } else {
                      syncNodeFrom(newContent, oldNode, ctx);
                      if (!ignoreValueOfActiveElement(oldNode, ctx)) {
                          morphChildren(newContent, oldNode, ctx);
                      }
                  }
                  ctx.callbacks.afterNodeMorphed(oldNode, newContent);
                  return oldNode;
              }
          }

          /**
           * This is the core algorithm for matching up children.  The idea is to use id sets to try to match up
           * nodes as faithfully as possible.  We greedily match, which allows us to keep the algorithm fast, but
           * by using id sets, we are able to better match up with content deeper in the DOM.
           *
           * Basic algorithm is, for each node in the new content:
           *
           * - if we have reached the end of the old parent, append the new content
           * - if the new content has an id set match with the current insertion point, morph
           * - search for an id set match
           * - if id set match found, morph
           * - otherwise search for a "soft" match
           * - if a soft match is found, morph
           * - otherwise, prepend the new node before the current insertion point
           *
           * The two search algorithms terminate if competing node matches appear to outweigh what can be achieved
           * with the current node.  See findIdSetMatch() and findSoftMatch() for details.
           *
           * @param {Element} newParent the parent element of the new content
           * @param {Element } oldParent the old content that we are merging the new content into
           * @param ctx the merge context
           */
          function morphChildren(newParent, oldParent, ctx) {

              let nextNewChild = newParent.firstChild;
              let insertionPoint = oldParent.firstChild;
              let newChild;

              // run through all the new content
              while (nextNewChild) {

                  newChild = nextNewChild;
                  nextNewChild = newChild.nextSibling;

                  // if we are at the end of the exiting parent's children, just append
                  if (insertionPoint == null) {
                      if (ctx.callbacks.beforeNodeAdded(newChild) === false) return;

                      oldParent.appendChild(newChild);
                      ctx.callbacks.afterNodeAdded(newChild);
                      removeIdsFromConsideration(ctx, newChild);
                      continue;
                  }

                  // if the current node has an id set match then morph
                  if (isIdSetMatch(newChild, insertionPoint, ctx)) {
                      morphOldNodeTo(insertionPoint, newChild, ctx);
                      insertionPoint = insertionPoint.nextSibling;
                      removeIdsFromConsideration(ctx, newChild);
                      continue;
                  }

                  // otherwise search forward in the existing old children for an id set match
                  let idSetMatch = findIdSetMatch(newParent, oldParent, newChild, insertionPoint, ctx);

                  // if we found a potential match, remove the nodes until that point and morph
                  if (idSetMatch) {
                      insertionPoint = removeNodesBetween(insertionPoint, idSetMatch, ctx);
                      morphOldNodeTo(idSetMatch, newChild, ctx);
                      removeIdsFromConsideration(ctx, newChild);
                      continue;
                  }

                  // no id set match found, so scan forward for a soft match for the current node
                  let softMatch = findSoftMatch(newParent, oldParent, newChild, insertionPoint, ctx);

                  // if we found a soft match for the current node, morph
                  if (softMatch) {
                      insertionPoint = removeNodesBetween(insertionPoint, softMatch, ctx);
                      morphOldNodeTo(softMatch, newChild, ctx);
                      removeIdsFromConsideration(ctx, newChild);
                      continue;
                  }

                  // abandon all hope of morphing, just insert the new child before the insertion point
                  // and move on
                  if (ctx.callbacks.beforeNodeAdded(newChild) === false) return;

                  oldParent.insertBefore(newChild, insertionPoint);
                  ctx.callbacks.afterNodeAdded(newChild);
                  removeIdsFromConsideration(ctx, newChild);
              }

              // remove any remaining old nodes that didn't match up with new content
              while (insertionPoint !== null) {

                  let tempNode = insertionPoint;
                  insertionPoint = insertionPoint.nextSibling;
                  removeNode(tempNode, ctx);
              }
          }

          //=============================================================================
          // Attribute Syncing Code
          //=============================================================================

          /**
           * @param attr {String} the attribute to be mutated
           * @param to {Element} the element that is going to be updated
           * @param updateType {("update"|"remove")}
           * @param ctx the merge context
           * @returns {boolean} true if the attribute should be ignored, false otherwise
           */
          function ignoreAttribute(attr, to, updateType, ctx) {
              if(attr === 'value' && ctx.ignoreActiveValue && to === document.activeElement){
                  return true;
              }
              return ctx.callbacks.beforeAttributeUpdated(attr, to, updateType) === false;
          }

          /**
           * syncs a given node with another node, copying over all attributes and
           * inner element state from the 'from' node to the 'to' node
           *
           * @param {Element} from the element to copy attributes & state from
           * @param {Element} to the element to copy attributes & state to
           * @param ctx the merge context
           */
          function syncNodeFrom(from, to, ctx) {
              let type = from.nodeType;

              // if is an element type, sync the attributes from the
              // new node into the new node
              if (type === 1 /* element type */) {
                  const fromAttributes = from.attributes;
                  const toAttributes = to.attributes;
                  for (const fromAttribute of fromAttributes) {
                      if (ignoreAttribute(fromAttribute.name, to, 'update', ctx)) {
                          continue;
                      }
                      if (to.getAttribute(fromAttribute.name) !== fromAttribute.value) {
                          to.setAttribute(fromAttribute.name, fromAttribute.value);
                      }
                  }
                  // iterate backwards to avoid skipping over items when a delete occurs
                  for (let i = toAttributes.length - 1; 0 <= i; i--) {
                      const toAttribute = toAttributes[i];
                      if (ignoreAttribute(toAttribute.name, to, 'remove', ctx)) {
                          continue;
                      }
                      if (!from.hasAttribute(toAttribute.name)) {
                          to.removeAttribute(toAttribute.name);
                      }
                  }
              }

              // sync text nodes
              if (type === 8 /* comment */ || type === 3 /* text */) {
                  if (to.nodeValue !== from.nodeValue) {
                      to.nodeValue = from.nodeValue;
                  }
              }

              if (!ignoreValueOfActiveElement(to, ctx)) {
                  // sync input values
                  syncInputValue(from, to, ctx);
              }
          }

          /**
           * @param from {Element} element to sync the value from
           * @param to {Element} element to sync the value to
           * @param attributeName {String} the attribute name
           * @param ctx the merge context
           */
          function syncBooleanAttribute(from, to, attributeName, ctx) {
              if (from[attributeName] !== to[attributeName]) {
                  let ignoreUpdate = ignoreAttribute(attributeName, to, 'update', ctx);
                  if (!ignoreUpdate) {
                      to[attributeName] = from[attributeName];
                  }
                  if (from[attributeName]) {
                      if (!ignoreUpdate) {
                          to.setAttribute(attributeName, from[attributeName]);
                      }
                  } else {
                      if (!ignoreAttribute(attributeName, to, 'remove', ctx)) {
                          to.removeAttribute(attributeName);
                      }
                  }
              }
          }

          /**
           * NB: many bothans died to bring us information:
           *
           *  https://github.com/patrick-steele-idem/morphdom/blob/master/src/specialElHandlers.js
           *  https://github.com/choojs/nanomorph/blob/master/lib/morph.jsL113
           *
           * @param from {Element} the element to sync the input value from
           * @param to {Element} the element to sync the input value to
           * @param ctx the merge context
           */
          function syncInputValue(from, to, ctx) {
              if (from instanceof HTMLInputElement &&
                  to instanceof HTMLInputElement &&
                  from.type !== 'file') {

                  let fromValue = from.value;
                  let toValue = to.value;

                  // sync boolean attributes
                  syncBooleanAttribute(from, to, 'checked', ctx);
                  syncBooleanAttribute(from, to, 'disabled', ctx);

                  if (!from.hasAttribute('value')) {
                      if (!ignoreAttribute('value', to, 'remove', ctx)) {
                          to.value = '';
                          to.removeAttribute('value');
                      }
                  } else if (fromValue !== toValue) {
                      if (!ignoreAttribute('value', to, 'update', ctx)) {
                          to.setAttribute('value', fromValue);
                          to.value = fromValue;
                      }
                  }
              } else if (from instanceof HTMLOptionElement) {
                  syncBooleanAttribute(from, to, 'selected', ctx);
              } else if (from instanceof HTMLTextAreaElement && to instanceof HTMLTextAreaElement) {
                  let fromValue = from.value;
                  let toValue = to.value;
                  if (ignoreAttribute('value', to, 'update', ctx)) {
                      return;
                  }
                  if (fromValue !== toValue) {
                      to.value = fromValue;
                  }
                  if (to.firstChild && to.firstChild.nodeValue !== fromValue) {
                      to.firstChild.nodeValue = fromValue;
                  }
              }
          }

          //=============================================================================
          // the HEAD tag can be handled specially, either w/ a 'merge' or 'append' style
          //=============================================================================
          function handleHeadElement(newHeadTag, currentHead, ctx) {

              let added = [];
              let removed = [];
              let preserved = [];
              let nodesToAppend = [];

              let headMergeStyle = ctx.head.style;

              // put all new head elements into a Map, by their outerHTML
              let srcToNewHeadNodes = new Map();
              for (const newHeadChild of newHeadTag.children) {
                  srcToNewHeadNodes.set(newHeadChild.outerHTML, newHeadChild);
              }

              // for each elt in the current head
              for (const currentHeadElt of currentHead.children) {

                  // If the current head element is in the map
                  let inNewContent = srcToNewHeadNodes.has(currentHeadElt.outerHTML);
                  let isReAppended = ctx.head.shouldReAppend(currentHeadElt);
                  let isPreserved = ctx.head.shouldPreserve(currentHeadElt);
                  if (inNewContent || isPreserved) {
                      if (isReAppended) {
                          // remove the current version and let the new version replace it and re-execute
                          removed.push(currentHeadElt);
                      } else {
                          // this element already exists and should not be re-appended, so remove it from
                          // the new content map, preserving it in the DOM
                          srcToNewHeadNodes.delete(currentHeadElt.outerHTML);
                          preserved.push(currentHeadElt);
                      }
                  } else {
                      if (headMergeStyle === "append") {
                          // we are appending and this existing element is not new content
                          // so if and only if it is marked for re-append do we do anything
                          if (isReAppended) {
                              removed.push(currentHeadElt);
                              nodesToAppend.push(currentHeadElt);
                          }
                      } else {
                          // if this is a merge, we remove this content since it is not in the new head
                          if (ctx.head.shouldRemove(currentHeadElt) !== false) {
                              removed.push(currentHeadElt);
                          }
                      }
                  }
              }

              // Push the remaining new head elements in the Map into the
              // nodes to append to the head tag
              nodesToAppend.push(...srcToNewHeadNodes.values());

              let promises = [];
              for (const newNode of nodesToAppend) {
                  let newElt = document.createRange().createContextualFragment(newNode.outerHTML).firstChild;
                  if (ctx.callbacks.beforeNodeAdded(newElt) !== false) {
                      if (newElt.href || newElt.src) {
                          let resolve = null;
                          let promise = new Promise(function (_resolve) {
                              resolve = _resolve;
                          });
                          newElt.addEventListener('load', function () {
                              resolve();
                          });
                          promises.push(promise);
                      }
                      currentHead.appendChild(newElt);
                      ctx.callbacks.afterNodeAdded(newElt);
                      added.push(newElt);
                  }
              }

              // remove all removed elements, after we have appended the new elements to avoid
              // additional network requests for things like style sheets
              for (const removedElement of removed) {
                  if (ctx.callbacks.beforeNodeRemoved(removedElement) !== false) {
                      currentHead.removeChild(removedElement);
                      ctx.callbacks.afterNodeRemoved(removedElement);
                  }
              }

              ctx.head.afterHeadMorphed(currentHead, {added: added, kept: preserved, removed: removed});
              return promises;
          }

          function noOp() {
          }

          /*
            Deep merges the config object and the Idiomoroph.defaults object to
            produce a final configuration object
           */
          function mergeDefaults(config) {
              let finalConfig = {};
              // copy top level stuff into final config
              Object.assign(finalConfig, defaults);
              Object.assign(finalConfig, config);

              // copy callbacks into final config (do this to deep merge the callbacks)
              finalConfig.callbacks = {};
              Object.assign(finalConfig.callbacks, defaults.callbacks);
              Object.assign(finalConfig.callbacks, config.callbacks);

              // copy head config into final config  (do this to deep merge the head)
              finalConfig.head = {};
              Object.assign(finalConfig.head, defaults.head);
              Object.assign(finalConfig.head, config.head);
              return finalConfig;
          }

          function createMorphContext(oldNode, newContent, config) {
              config = mergeDefaults(config);
              return {
                  target: oldNode,
                  newContent: newContent,
                  config: config,
                  morphStyle: config.morphStyle,
                  ignoreActive: config.ignoreActive,
                  ignoreActiveValue: config.ignoreActiveValue,
                  idMap: createIdMap(oldNode, newContent),
                  deadIds: new Set(),
                  callbacks: config.callbacks,
                  head: config.head
              }
          }

          function isIdSetMatch(node1, node2, ctx) {
              if (node1 == null || node2 == null) {
                  return false;
              }
              if (node1.nodeType === node2.nodeType && node1.tagName === node2.tagName) {
                  if (node1.id !== "" && node1.id === node2.id) {
                      return true;
                  } else {
                      return getIdIntersectionCount(ctx, node1, node2) > 0;
                  }
              }
              return false;
          }

          function isSoftMatch(node1, node2) {
              if (node1 == null || node2 == null) {
                  return false;
              }
              return node1.nodeType === node2.nodeType && node1.tagName === node2.tagName
          }

          function removeNodesBetween(startInclusive, endExclusive, ctx) {
              while (startInclusive !== endExclusive) {
                  let tempNode = startInclusive;
                  startInclusive = startInclusive.nextSibling;
                  removeNode(tempNode, ctx);
              }
              removeIdsFromConsideration(ctx, endExclusive);
              return endExclusive.nextSibling;
          }

          //=============================================================================
          // Scans forward from the insertionPoint in the old parent looking for a potential id match
          // for the newChild.  We stop if we find a potential id match for the new child OR
          // if the number of potential id matches we are discarding is greater than the
          // potential id matches for the new child
          //=============================================================================
          function findIdSetMatch(newContent, oldParent, newChild, insertionPoint, ctx) {

              // max id matches we are willing to discard in our search
              let newChildPotentialIdCount = getIdIntersectionCount(ctx, newChild, oldParent);

              let potentialMatch = null;

              // only search forward if there is a possibility of an id match
              if (newChildPotentialIdCount > 0) {
                  let potentialMatch = insertionPoint;
                  // if there is a possibility of an id match, scan forward
                  // keep track of the potential id match count we are discarding (the
                  // newChildPotentialIdCount must be greater than this to make it likely
                  // worth it)
                  let otherMatchCount = 0;
                  while (potentialMatch != null) {

                      // If we have an id match, return the current potential match
                      if (isIdSetMatch(newChild, potentialMatch, ctx)) {
                          return potentialMatch;
                      }

                      // computer the other potential matches of this new content
                      otherMatchCount += getIdIntersectionCount(ctx, potentialMatch, newContent);
                      if (otherMatchCount > newChildPotentialIdCount) {
                          // if we have more potential id matches in _other_ content, we
                          // do not have a good candidate for an id match, so return null
                          return null;
                      }

                      // advanced to the next old content child
                      potentialMatch = potentialMatch.nextSibling;
                  }
              }
              return potentialMatch;
          }

          //=============================================================================
          // Scans forward from the insertionPoint in the old parent looking for a potential soft match
          // for the newChild.  We stop if we find a potential soft match for the new child OR
          // if we find a potential id match in the old parents children OR if we find two
          // potential soft matches for the next two pieces of new content
          //=============================================================================
          function findSoftMatch(newContent, oldParent, newChild, insertionPoint, ctx) {

              let potentialSoftMatch = insertionPoint;
              let nextSibling = newChild.nextSibling;
              let siblingSoftMatchCount = 0;

              while (potentialSoftMatch != null) {

                  if (getIdIntersectionCount(ctx, potentialSoftMatch, newContent) > 0) {
                      // the current potential soft match has a potential id set match with the remaining new
                      // content so bail out of looking
                      return null;
                  }

                  // if we have a soft match with the current node, return it
                  if (isSoftMatch(newChild, potentialSoftMatch)) {
                      return potentialSoftMatch;
                  }

                  if (isSoftMatch(nextSibling, potentialSoftMatch)) {
                      // the next new node has a soft match with this node, so
                      // increment the count of future soft matches
                      siblingSoftMatchCount++;
                      nextSibling = nextSibling.nextSibling;

                      // If there are two future soft matches, bail to allow the siblings to soft match
                      // so that we don't consume future soft matches for the sake of the current node
                      if (siblingSoftMatchCount >= 2) {
                          return null;
                      }
                  }

                  // advanced to the next old content child
                  potentialSoftMatch = potentialSoftMatch.nextSibling;
              }

              return potentialSoftMatch;
          }

          function parseContent(newContent) {
              let parser = new DOMParser();

              // remove svgs to avoid false-positive matches on head, etc.
              let contentWithSvgsRemoved = newContent.replace(/<svg(\s[^>]*>|>)([\s\S]*?)<\/svg>/gim, '');

              // if the newContent contains a html, head or body tag, we can simply parse it w/o wrapping
              if (contentWithSvgsRemoved.match(/<\/html>/) || contentWithSvgsRemoved.match(/<\/head>/) || contentWithSvgsRemoved.match(/<\/body>/)) {
                  let content = parser.parseFromString(newContent, "text/html");
                  // if it is a full HTML document, return the document itself as the parent container
                  if (contentWithSvgsRemoved.match(/<\/html>/)) {
                      content.generatedByIdiomorph = true;
                      return content;
                  } else {
                      // otherwise return the html element as the parent container
                      let htmlElement = content.firstChild;
                      if (htmlElement) {
                          htmlElement.generatedByIdiomorph = true;
                          return htmlElement;
                      } else {
                          return null;
                      }
                  }
              } else {
                  // if it is partial HTML, wrap it in a template tag to provide a parent element and also to help
                  // deal with touchy tags like tr, tbody, etc.
                  let responseDoc = parser.parseFromString("<body><template>" + newContent + "</template></body>", "text/html");
                  let content = responseDoc.body.querySelector('template').content;
                  content.generatedByIdiomorph = true;
                  return content
              }
          }

          function normalizeContent(newContent) {
              if (newContent == null) {
                  // noinspection UnnecessaryLocalVariableJS
                  const dummyParent = document.createElement('div');
                  return dummyParent;
              } else if (newContent.generatedByIdiomorph) {
                  // the template tag created by idiomorph parsing can serve as a dummy parent
                  return newContent;
              } else if (newContent instanceof Node) {
                  // a single node is added as a child to a dummy parent
                  const dummyParent = document.createElement('div');
                  dummyParent.append(newContent);
                  return dummyParent;
              } else {
                  // all nodes in the array or HTMLElement collection are consolidated under
                  // a single dummy parent element
                  const dummyParent = document.createElement('div');
                  for (const elt of [...newContent]) {
                      dummyParent.append(elt);
                  }
                  return dummyParent;
              }
          }

          function insertSiblings(previousSibling, morphedNode, nextSibling) {
              let stack = [];
              let added = [];
              while (previousSibling != null) {
                  stack.push(previousSibling);
                  previousSibling = previousSibling.previousSibling;
              }
              while (stack.length > 0) {
                  let node = stack.pop();
                  added.push(node); // push added preceding siblings on in order and insert
                  morphedNode.parentElement.insertBefore(node, morphedNode);
              }
              added.push(morphedNode);
              while (nextSibling != null) {
                  stack.push(nextSibling);
                  added.push(nextSibling); // here we are going in order, so push on as we scan, rather than add
                  nextSibling = nextSibling.nextSibling;
              }
              while (stack.length > 0) {
                  morphedNode.parentElement.insertBefore(stack.pop(), morphedNode.nextSibling);
              }
              return added;
          }

          function findBestNodeMatch(newContent, oldNode, ctx) {
              let currentElement;
              currentElement = newContent.firstChild;
              let bestElement = currentElement;
              let score = 0;
              while (currentElement) {
                  let newScore = scoreElement(currentElement, oldNode, ctx);
                  if (newScore > score) {
                      bestElement = currentElement;
                      score = newScore;
                  }
                  currentElement = currentElement.nextSibling;
              }
              return bestElement;
          }

          function scoreElement(node1, node2, ctx) {
              if (isSoftMatch(node1, node2)) {
                  return .5 + getIdIntersectionCount(ctx, node1, node2);
              }
              return 0;
          }

          function removeNode(tempNode, ctx) {
              removeIdsFromConsideration(ctx, tempNode);
              if (ctx.callbacks.beforeNodeRemoved(tempNode) === false) return;

              tempNode.remove();
              ctx.callbacks.afterNodeRemoved(tempNode);
          }

          //=============================================================================
          // ID Set Functions
          //=============================================================================

          function isIdInConsideration(ctx, id) {
              return !ctx.deadIds.has(id);
          }

          function idIsWithinNode(ctx, id, targetNode) {
              let idSet = ctx.idMap.get(targetNode) || EMPTY_SET;
              return idSet.has(id);
          }

          function removeIdsFromConsideration(ctx, node) {
              let idSet = ctx.idMap.get(node) || EMPTY_SET;
              for (const id of idSet) {
                  ctx.deadIds.add(id);
              }
          }

          function getIdIntersectionCount(ctx, node1, node2) {
              let sourceSet = ctx.idMap.get(node1) || EMPTY_SET;
              let matchCount = 0;
              for (const id of sourceSet) {
                  // a potential match is an id in the source and potentialIdsSet, but
                  // that has not already been merged into the DOM
                  if (isIdInConsideration(ctx, id) && idIsWithinNode(ctx, id, node2)) {
                      ++matchCount;
                  }
              }
              return matchCount;
          }

          /**
           * A bottom up algorithm that finds all elements with ids inside of the node
           * argument and populates id sets for those nodes and all their parents, generating
           * a set of ids contained within all nodes for the entire hierarchy in the DOM
           *
           * @param node {Element}
           * @param {Map<Node, Set<String>>} idMap
           */
          function populateIdMapForNode(node, idMap) {
              let nodeParent = node.parentElement;
              // find all elements with an id property
              let idElements = node.querySelectorAll('[id]');
              for (const elt of idElements) {
                  let current = elt;
                  // walk up the parent hierarchy of that element, adding the id
                  // of element to the parent's id set
                  while (current !== nodeParent && current != null) {
                      let idSet = idMap.get(current);
                      // if the id set doesn't exist, create it and insert it in the  map
                      if (idSet == null) {
                          idSet = new Set();
                          idMap.set(current, idSet);
                      }
                      idSet.add(elt.id);
                      current = current.parentElement;
                  }
              }
          }

          /**
           * This function computes a map of nodes to all ids contained within that node (inclusive of the
           * node).  This map can be used to ask if two nodes have intersecting sets of ids, which allows
           * for a looser definition of "matching" than tradition id matching, and allows child nodes
           * to contribute to a parent nodes matching.
           *
           * @param {Element} oldContent  the old content that will be morphed
           * @param {Element} newContent  the new content to morph to
           * @returns {Map<Node, Set<String>>} a map of nodes to id sets for the
           */
          function createIdMap(oldContent, newContent) {
              let idMap = new Map();
              populateIdMapForNode(oldContent, idMap);
              populateIdMapForNode(newContent, idMap);
              return idMap;
          }

          //=============================================================================
          // This is what ends up becoming the Idiomorph global object
          //=============================================================================
          return {
              morph,
              defaults
          }
      })();

  function log() {
    if (HotwireSpark.config.loggingEnabled) {
      for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
        args[_key] = arguments[_key];
      }
      console.log(`[hotwire_spark]`, ...args);
    }
  }

  // import { Application } from "@hotwired/stimulus"
  class StimulusReloader {
    static async reload(filePattern) {
      const document = await reloadHtmlDocument();
      return new StimulusReloader(document, filePattern).reload();
    }
    constructor(document) {
      let filePattern = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : /./;
      this.document = document;
      this.filePattern = filePattern;
      this.application = window.Stimulus || Application.start();
    }
    async reload() {
      log("Reload Stimulus controllers...");
      this.application.stop();
      await this.#reloadStimulusControllers();
      this.application.start();
    }
    async #reloadStimulusControllers() {
      await Promise.all(this.#stimulusControllerPaths.map(async moduleName => this.#reloadStimulusController(moduleName)));
    }
    get #stimulusControllerPaths() {
      return Object.keys(this.#stimulusPathsByModule).filter(path => path.endsWith("_controller") && this.#shouldReloadController(path));
    }
    #shouldReloadController(path) {
      return this.filePattern.test(path);
    }
    get #stimulusPathsByModule() {
      this.pathsByModule = this.pathsByModule || this.#parseImportmapJson();
      return this.pathsByModule;
    }
    #parseImportmapJson() {
      const importmapScript = this.document.querySelector("script[type=importmap]");
      return JSON.parse(importmapScript.text).imports;
    }
    async #reloadStimulusController(moduleName) {
      log(`\t${moduleName}`);
      const controllerName = this.#extractControllerName(moduleName);
      const path = cacheBustedUrl(this.#pathForModuleName(moduleName));
      const module = await import(path);
      this.#registerController(controllerName, module);
    }
    #pathForModuleName(moduleName) {
      return this.#stimulusPathsByModule[moduleName];
    }
    #extractControllerName(path) {
      return path.replace(/^.*\//, "").replace("_controller", "").replace(/\//g, "--").replace(/_/g, "-");
    }
    #registerController(name, module) {
      this.application.unload(name);
      this.application.register(name, module.default);
    }
  }

  class HtmlReloader {
    static async reload() {
      return new HtmlReloader().reload();
    }
    async reload() {
      const reloadedDocument = await this.#reloadHtml();
      await this.#reloadStimulus(reloadedDocument);
    }
    async #reloadHtml() {
      log("Reload html...");
      const reloadedDocument = await reloadHtmlDocument();
      this.#updateBody(reloadedDocument.body);
      return reloadedDocument;
    }
    #updateBody(newBody) {
      Idiomorph.morph(document.body, newBody);
    }
    async #reloadStimulus(reloadedDocument) {
      return new StimulusReloader(reloadedDocument).reload();
    }
  }

  class CssReloader {
    static async reload() {
      for (var _len = arguments.length, params = new Array(_len), _key = 0; _key < _len; _key++) {
        params[_key] = arguments[_key];
      }
      return new CssReloader(...params).reload();
    }
    constructor() {
      let filePattern = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : /./;
      this.filePattern = filePattern;
    }
    async reload() {
      log("Reload css...");
      await Promise.all(await this.#reloadAllLinks());
    }
    async #reloadAllLinks() {
      const cssLinks = await this.#loadNewCssLinks();
      return cssLinks.map(link => this.#reloadLinkIfNeeded(link));
    }
    async #loadNewCssLinks() {
      const reloadedDocument = await reloadHtmlDocument();
      return Array.from(reloadedDocument.head.querySelectorAll("link[rel='stylesheet']"));
    }
    #reloadLinkIfNeeded(link) {
      if (this.#shouldReloadLink(link)) {
        return this.#reloadLink(link);
      } else {
        return Promise.resolve();
      }
    }
    #shouldReloadLink(link) {
      return this.filePattern.test(link.getAttribute("href"));
    }
    async #reloadLink(link) {
      return new Promise(resolve => {
        const href = link.getAttribute("href");
        const newLink = this.#findExistingLinkFor(link) || this.#appendNewLink(link);
        newLink.setAttribute("href", cacheBustedUrl(link.getAttribute("href")));
        newLink.onload = () => {
          log(`\t${href}`);
          resolve();
        };
      });
    }
    #findExistingLinkFor(link) {
      return this.#cssLinks.find(newLink => pathWithoutAssetDigest(link.href) === pathWithoutAssetDigest(newLink.href));
    }
    get #cssLinks() {
      return Array.from(document.querySelectorAll("link[rel='stylesheet']"));
    }
    #appendNewLink(link) {
      document.head.append(link);
      return link;
    }
  }

  consumer.subscriptions.create({
    channel: "Hotwire::Spark::Channel"
  }, {
    connected() {
      document.body.setAttribute("data-hotwire-spark-ready", "");
    },
    async received(message) {
      try {
        await this.dispatch(message);
      } catch (error) {
        console.log(`Error on ${message.action}`, error);
      }
    },
    dispatch(_ref) {
      let {
        action,
        path
      } = _ref;
      const fileName = assetNameFromPath(path);
      switch (action) {
        case "reload_html":
          return this.reloadHtml();
        case "reload_css":
          return this.reloadCss(fileName);
        case "reload_stimulus":
          return this.reloadStimulus(fileName);
        default:
          throw new Error(`Unknown action: ${action}`);
      }
    },
    reloadHtml() {
      return HtmlReloader.reload();
    },
    reloadCss(fileName) {
      return CssReloader.reload(new RegExp(fileName));
    },
    reloadStimulus(fileName) {
      return StimulusReloader.reload(new RegExp(fileName));
    }
  });

  const HotwireSpark = {
    config: {
      loggingEnabled: false
    }
  };
  document.addEventListener("DOMContentLoaded", function () {
    HotwireSpark.config.loggingEnabled = getConfigurationProperty("logging");
  });

  return HotwireSpark;

})();
