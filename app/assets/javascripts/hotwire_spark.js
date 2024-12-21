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

  const extend$1 = function(object, properties) {
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
      extend$1(this, mixin);
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

  var consumer = createConsumer("/hotwire-spark");

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

  /*
  Stimulus 3.2.1
  Copyright © 2023 Basecamp, LLC
   */
  class EventListener {
      constructor(eventTarget, eventName, eventOptions) {
          this.eventTarget = eventTarget;
          this.eventName = eventName;
          this.eventOptions = eventOptions;
          this.unorderedBindings = new Set();
      }
      connect() {
          this.eventTarget.addEventListener(this.eventName, this, this.eventOptions);
      }
      disconnect() {
          this.eventTarget.removeEventListener(this.eventName, this, this.eventOptions);
      }
      bindingConnected(binding) {
          this.unorderedBindings.add(binding);
      }
      bindingDisconnected(binding) {
          this.unorderedBindings.delete(binding);
      }
      handleEvent(event) {
          const extendedEvent = extendEvent(event);
          for (const binding of this.bindings) {
              if (extendedEvent.immediatePropagationStopped) {
                  break;
              }
              else {
                  binding.handleEvent(extendedEvent);
              }
          }
      }
      hasBindings() {
          return this.unorderedBindings.size > 0;
      }
      get bindings() {
          return Array.from(this.unorderedBindings).sort((left, right) => {
              const leftIndex = left.index, rightIndex = right.index;
              return leftIndex < rightIndex ? -1 : leftIndex > rightIndex ? 1 : 0;
          });
      }
  }
  function extendEvent(event) {
      if ("immediatePropagationStopped" in event) {
          return event;
      }
      else {
          const { stopImmediatePropagation } = event;
          return Object.assign(event, {
              immediatePropagationStopped: false,
              stopImmediatePropagation() {
                  this.immediatePropagationStopped = true;
                  stopImmediatePropagation.call(this);
              },
          });
      }
  }

  class Dispatcher {
      constructor(application) {
          this.application = application;
          this.eventListenerMaps = new Map();
          this.started = false;
      }
      start() {
          if (!this.started) {
              this.started = true;
              this.eventListeners.forEach((eventListener) => eventListener.connect());
          }
      }
      stop() {
          if (this.started) {
              this.started = false;
              this.eventListeners.forEach((eventListener) => eventListener.disconnect());
          }
      }
      get eventListeners() {
          return Array.from(this.eventListenerMaps.values()).reduce((listeners, map) => listeners.concat(Array.from(map.values())), []);
      }
      bindingConnected(binding) {
          this.fetchEventListenerForBinding(binding).bindingConnected(binding);
      }
      bindingDisconnected(binding, clearEventListeners = false) {
          this.fetchEventListenerForBinding(binding).bindingDisconnected(binding);
          if (clearEventListeners)
              this.clearEventListenersForBinding(binding);
      }
      handleError(error, message, detail = {}) {
          this.application.handleError(error, `Error ${message}`, detail);
      }
      clearEventListenersForBinding(binding) {
          const eventListener = this.fetchEventListenerForBinding(binding);
          if (!eventListener.hasBindings()) {
              eventListener.disconnect();
              this.removeMappedEventListenerFor(binding);
          }
      }
      removeMappedEventListenerFor(binding) {
          const { eventTarget, eventName, eventOptions } = binding;
          const eventListenerMap = this.fetchEventListenerMapForEventTarget(eventTarget);
          const cacheKey = this.cacheKey(eventName, eventOptions);
          eventListenerMap.delete(cacheKey);
          if (eventListenerMap.size == 0)
              this.eventListenerMaps.delete(eventTarget);
      }
      fetchEventListenerForBinding(binding) {
          const { eventTarget, eventName, eventOptions } = binding;
          return this.fetchEventListener(eventTarget, eventName, eventOptions);
      }
      fetchEventListener(eventTarget, eventName, eventOptions) {
          const eventListenerMap = this.fetchEventListenerMapForEventTarget(eventTarget);
          const cacheKey = this.cacheKey(eventName, eventOptions);
          let eventListener = eventListenerMap.get(cacheKey);
          if (!eventListener) {
              eventListener = this.createEventListener(eventTarget, eventName, eventOptions);
              eventListenerMap.set(cacheKey, eventListener);
          }
          return eventListener;
      }
      createEventListener(eventTarget, eventName, eventOptions) {
          const eventListener = new EventListener(eventTarget, eventName, eventOptions);
          if (this.started) {
              eventListener.connect();
          }
          return eventListener;
      }
      fetchEventListenerMapForEventTarget(eventTarget) {
          let eventListenerMap = this.eventListenerMaps.get(eventTarget);
          if (!eventListenerMap) {
              eventListenerMap = new Map();
              this.eventListenerMaps.set(eventTarget, eventListenerMap);
          }
          return eventListenerMap;
      }
      cacheKey(eventName, eventOptions) {
          const parts = [eventName];
          Object.keys(eventOptions)
              .sort()
              .forEach((key) => {
              parts.push(`${eventOptions[key] ? "" : "!"}${key}`);
          });
          return parts.join(":");
      }
  }

  const defaultActionDescriptorFilters = {
      stop({ event, value }) {
          if (value)
              event.stopPropagation();
          return true;
      },
      prevent({ event, value }) {
          if (value)
              event.preventDefault();
          return true;
      },
      self({ event, value, element }) {
          if (value) {
              return element === event.target;
          }
          else {
              return true;
          }
      },
  };
  const descriptorPattern = /^(?:(?:([^.]+?)\+)?(.+?)(?:\.(.+?))?(?:@(window|document))?->)?(.+?)(?:#([^:]+?))(?::(.+))?$/;
  function parseActionDescriptorString(descriptorString) {
      const source = descriptorString.trim();
      const matches = source.match(descriptorPattern) || [];
      let eventName = matches[2];
      let keyFilter = matches[3];
      if (keyFilter && !["keydown", "keyup", "keypress"].includes(eventName)) {
          eventName += `.${keyFilter}`;
          keyFilter = "";
      }
      return {
          eventTarget: parseEventTarget(matches[4]),
          eventName,
          eventOptions: matches[7] ? parseEventOptions(matches[7]) : {},
          identifier: matches[5],
          methodName: matches[6],
          keyFilter: matches[1] || keyFilter,
      };
  }
  function parseEventTarget(eventTargetName) {
      if (eventTargetName == "window") {
          return window;
      }
      else if (eventTargetName == "document") {
          return document;
      }
  }
  function parseEventOptions(eventOptions) {
      return eventOptions
          .split(":")
          .reduce((options, token) => Object.assign(options, { [token.replace(/^!/, "")]: !/^!/.test(token) }), {});
  }
  function stringifyEventTarget(eventTarget) {
      if (eventTarget == window) {
          return "window";
      }
      else if (eventTarget == document) {
          return "document";
      }
  }

  function camelize(value) {
      return value.replace(/(?:[_-])([a-z0-9])/g, (_, char) => char.toUpperCase());
  }
  function namespaceCamelize(value) {
      return camelize(value.replace(/--/g, "-").replace(/__/g, "_"));
  }
  function capitalize(value) {
      return value.charAt(0).toUpperCase() + value.slice(1);
  }
  function dasherize(value) {
      return value.replace(/([A-Z])/g, (_, char) => `-${char.toLowerCase()}`);
  }
  function tokenize(value) {
      return value.match(/[^\s]+/g) || [];
  }
  function hasProperty(object, property) {
      return Object.prototype.hasOwnProperty.call(object, property);
  }

  const allModifiers = ["meta", "ctrl", "alt", "shift"];
  class Action {
      constructor(element, index, descriptor, schema) {
          this.element = element;
          this.index = index;
          this.eventTarget = descriptor.eventTarget || element;
          this.eventName = descriptor.eventName || getDefaultEventNameForElement(element) || error("missing event name");
          this.eventOptions = descriptor.eventOptions || {};
          this.identifier = descriptor.identifier || error("missing identifier");
          this.methodName = descriptor.methodName || error("missing method name");
          this.keyFilter = descriptor.keyFilter || "";
          this.schema = schema;
      }
      static forToken(token, schema) {
          return new this(token.element, token.index, parseActionDescriptorString(token.content), schema);
      }
      toString() {
          const eventFilter = this.keyFilter ? `.${this.keyFilter}` : "";
          const eventTarget = this.eventTargetName ? `@${this.eventTargetName}` : "";
          return `${this.eventName}${eventFilter}${eventTarget}->${this.identifier}#${this.methodName}`;
      }
      shouldIgnoreKeyboardEvent(event) {
          if (!this.keyFilter) {
              return false;
          }
          const filters = this.keyFilter.split("+");
          if (this.keyFilterDissatisfied(event, filters)) {
              return true;
          }
          const standardFilter = filters.filter((key) => !allModifiers.includes(key))[0];
          if (!standardFilter) {
              return false;
          }
          if (!hasProperty(this.keyMappings, standardFilter)) {
              error(`contains unknown key filter: ${this.keyFilter}`);
          }
          return this.keyMappings[standardFilter].toLowerCase() !== event.key.toLowerCase();
      }
      shouldIgnoreMouseEvent(event) {
          if (!this.keyFilter) {
              return false;
          }
          const filters = [this.keyFilter];
          if (this.keyFilterDissatisfied(event, filters)) {
              return true;
          }
          return false;
      }
      get params() {
          const params = {};
          const pattern = new RegExp(`^data-${this.identifier}-(.+)-param$`, "i");
          for (const { name, value } of Array.from(this.element.attributes)) {
              const match = name.match(pattern);
              const key = match && match[1];
              if (key) {
                  params[camelize(key)] = typecast(value);
              }
          }
          return params;
      }
      get eventTargetName() {
          return stringifyEventTarget(this.eventTarget);
      }
      get keyMappings() {
          return this.schema.keyMappings;
      }
      keyFilterDissatisfied(event, filters) {
          const [meta, ctrl, alt, shift] = allModifiers.map((modifier) => filters.includes(modifier));
          return event.metaKey !== meta || event.ctrlKey !== ctrl || event.altKey !== alt || event.shiftKey !== shift;
      }
  }
  const defaultEventNames = {
      a: () => "click",
      button: () => "click",
      form: () => "submit",
      details: () => "toggle",
      input: (e) => (e.getAttribute("type") == "submit" ? "click" : "input"),
      select: () => "change",
      textarea: () => "input",
  };
  function getDefaultEventNameForElement(element) {
      const tagName = element.tagName.toLowerCase();
      if (tagName in defaultEventNames) {
          return defaultEventNames[tagName](element);
      }
  }
  function error(message) {
      throw new Error(message);
  }
  function typecast(value) {
      try {
          return JSON.parse(value);
      }
      catch (o_O) {
          return value;
      }
  }

  class Binding {
      constructor(context, action) {
          this.context = context;
          this.action = action;
      }
      get index() {
          return this.action.index;
      }
      get eventTarget() {
          return this.action.eventTarget;
      }
      get eventOptions() {
          return this.action.eventOptions;
      }
      get identifier() {
          return this.context.identifier;
      }
      handleEvent(event) {
          const actionEvent = this.prepareActionEvent(event);
          if (this.willBeInvokedByEvent(event) && this.applyEventModifiers(actionEvent)) {
              this.invokeWithEvent(actionEvent);
          }
      }
      get eventName() {
          return this.action.eventName;
      }
      get method() {
          const method = this.controller[this.methodName];
          if (typeof method == "function") {
              return method;
          }
          throw new Error(`Action "${this.action}" references undefined method "${this.methodName}"`);
      }
      applyEventModifiers(event) {
          const { element } = this.action;
          const { actionDescriptorFilters } = this.context.application;
          const { controller } = this.context;
          let passes = true;
          for (const [name, value] of Object.entries(this.eventOptions)) {
              if (name in actionDescriptorFilters) {
                  const filter = actionDescriptorFilters[name];
                  passes = passes && filter({ name, value, event, element, controller });
              }
              else {
                  continue;
              }
          }
          return passes;
      }
      prepareActionEvent(event) {
          return Object.assign(event, { params: this.action.params });
      }
      invokeWithEvent(event) {
          const { target, currentTarget } = event;
          try {
              this.method.call(this.controller, event);
              this.context.logDebugActivity(this.methodName, { event, target, currentTarget, action: this.methodName });
          }
          catch (error) {
              const { identifier, controller, element, index } = this;
              const detail = { identifier, controller, element, index, event };
              this.context.handleError(error, `invoking action "${this.action}"`, detail);
          }
      }
      willBeInvokedByEvent(event) {
          const eventTarget = event.target;
          if (event instanceof KeyboardEvent && this.action.shouldIgnoreKeyboardEvent(event)) {
              return false;
          }
          if (event instanceof MouseEvent && this.action.shouldIgnoreMouseEvent(event)) {
              return false;
          }
          if (this.element === eventTarget) {
              return true;
          }
          else if (eventTarget instanceof Element && this.element.contains(eventTarget)) {
              return this.scope.containsElement(eventTarget);
          }
          else {
              return this.scope.containsElement(this.action.element);
          }
      }
      get controller() {
          return this.context.controller;
      }
      get methodName() {
          return this.action.methodName;
      }
      get element() {
          return this.scope.element;
      }
      get scope() {
          return this.context.scope;
      }
  }

  class ElementObserver {
      constructor(element, delegate) {
          this.mutationObserverInit = { attributes: true, childList: true, subtree: true };
          this.element = element;
          this.started = false;
          this.delegate = delegate;
          this.elements = new Set();
          this.mutationObserver = new MutationObserver((mutations) => this.processMutations(mutations));
      }
      start() {
          if (!this.started) {
              this.started = true;
              this.mutationObserver.observe(this.element, this.mutationObserverInit);
              this.refresh();
          }
      }
      pause(callback) {
          if (this.started) {
              this.mutationObserver.disconnect();
              this.started = false;
          }
          callback();
          if (!this.started) {
              this.mutationObserver.observe(this.element, this.mutationObserverInit);
              this.started = true;
          }
      }
      stop() {
          if (this.started) {
              this.mutationObserver.takeRecords();
              this.mutationObserver.disconnect();
              this.started = false;
          }
      }
      refresh() {
          if (this.started) {
              const matches = new Set(this.matchElementsInTree());
              for (const element of Array.from(this.elements)) {
                  if (!matches.has(element)) {
                      this.removeElement(element);
                  }
              }
              for (const element of Array.from(matches)) {
                  this.addElement(element);
              }
          }
      }
      processMutations(mutations) {
          if (this.started) {
              for (const mutation of mutations) {
                  this.processMutation(mutation);
              }
          }
      }
      processMutation(mutation) {
          if (mutation.type == "attributes") {
              this.processAttributeChange(mutation.target, mutation.attributeName);
          }
          else if (mutation.type == "childList") {
              this.processRemovedNodes(mutation.removedNodes);
              this.processAddedNodes(mutation.addedNodes);
          }
      }
      processAttributeChange(element, attributeName) {
          if (this.elements.has(element)) {
              if (this.delegate.elementAttributeChanged && this.matchElement(element)) {
                  this.delegate.elementAttributeChanged(element, attributeName);
              }
              else {
                  this.removeElement(element);
              }
          }
          else if (this.matchElement(element)) {
              this.addElement(element);
          }
      }
      processRemovedNodes(nodes) {
          for (const node of Array.from(nodes)) {
              const element = this.elementFromNode(node);
              if (element) {
                  this.processTree(element, this.removeElement);
              }
          }
      }
      processAddedNodes(nodes) {
          for (const node of Array.from(nodes)) {
              const element = this.elementFromNode(node);
              if (element && this.elementIsActive(element)) {
                  this.processTree(element, this.addElement);
              }
          }
      }
      matchElement(element) {
          return this.delegate.matchElement(element);
      }
      matchElementsInTree(tree = this.element) {
          return this.delegate.matchElementsInTree(tree);
      }
      processTree(tree, processor) {
          for (const element of this.matchElementsInTree(tree)) {
              processor.call(this, element);
          }
      }
      elementFromNode(node) {
          if (node.nodeType == Node.ELEMENT_NODE) {
              return node;
          }
      }
      elementIsActive(element) {
          if (element.isConnected != this.element.isConnected) {
              return false;
          }
          else {
              return this.element.contains(element);
          }
      }
      addElement(element) {
          if (!this.elements.has(element)) {
              if (this.elementIsActive(element)) {
                  this.elements.add(element);
                  if (this.delegate.elementMatched) {
                      this.delegate.elementMatched(element);
                  }
              }
          }
      }
      removeElement(element) {
          if (this.elements.has(element)) {
              this.elements.delete(element);
              if (this.delegate.elementUnmatched) {
                  this.delegate.elementUnmatched(element);
              }
          }
      }
  }

  class AttributeObserver {
      constructor(element, attributeName, delegate) {
          this.attributeName = attributeName;
          this.delegate = delegate;
          this.elementObserver = new ElementObserver(element, this);
      }
      get element() {
          return this.elementObserver.element;
      }
      get selector() {
          return `[${this.attributeName}]`;
      }
      start() {
          this.elementObserver.start();
      }
      pause(callback) {
          this.elementObserver.pause(callback);
      }
      stop() {
          this.elementObserver.stop();
      }
      refresh() {
          this.elementObserver.refresh();
      }
      get started() {
          return this.elementObserver.started;
      }
      matchElement(element) {
          return element.hasAttribute(this.attributeName);
      }
      matchElementsInTree(tree) {
          const match = this.matchElement(tree) ? [tree] : [];
          const matches = Array.from(tree.querySelectorAll(this.selector));
          return match.concat(matches);
      }
      elementMatched(element) {
          if (this.delegate.elementMatchedAttribute) {
              this.delegate.elementMatchedAttribute(element, this.attributeName);
          }
      }
      elementUnmatched(element) {
          if (this.delegate.elementUnmatchedAttribute) {
              this.delegate.elementUnmatchedAttribute(element, this.attributeName);
          }
      }
      elementAttributeChanged(element, attributeName) {
          if (this.delegate.elementAttributeValueChanged && this.attributeName == attributeName) {
              this.delegate.elementAttributeValueChanged(element, attributeName);
          }
      }
  }

  function add(map, key, value) {
      fetch$1(map, key).add(value);
  }
  function del(map, key, value) {
      fetch$1(map, key).delete(value);
      prune(map, key);
  }
  function fetch$1(map, key) {
      let values = map.get(key);
      if (!values) {
          values = new Set();
          map.set(key, values);
      }
      return values;
  }
  function prune(map, key) {
      const values = map.get(key);
      if (values != null && values.size == 0) {
          map.delete(key);
      }
  }

  class Multimap {
      constructor() {
          this.valuesByKey = new Map();
      }
      get keys() {
          return Array.from(this.valuesByKey.keys());
      }
      get values() {
          const sets = Array.from(this.valuesByKey.values());
          return sets.reduce((values, set) => values.concat(Array.from(set)), []);
      }
      get size() {
          const sets = Array.from(this.valuesByKey.values());
          return sets.reduce((size, set) => size + set.size, 0);
      }
      add(key, value) {
          add(this.valuesByKey, key, value);
      }
      delete(key, value) {
          del(this.valuesByKey, key, value);
      }
      has(key, value) {
          const values = this.valuesByKey.get(key);
          return values != null && values.has(value);
      }
      hasKey(key) {
          return this.valuesByKey.has(key);
      }
      hasValue(value) {
          const sets = Array.from(this.valuesByKey.values());
          return sets.some((set) => set.has(value));
      }
      getValuesForKey(key) {
          const values = this.valuesByKey.get(key);
          return values ? Array.from(values) : [];
      }
      getKeysForValue(value) {
          return Array.from(this.valuesByKey)
              .filter(([_key, values]) => values.has(value))
              .map(([key, _values]) => key);
      }
  }

  class SelectorObserver {
      constructor(element, selector, delegate, details) {
          this._selector = selector;
          this.details = details;
          this.elementObserver = new ElementObserver(element, this);
          this.delegate = delegate;
          this.matchesByElement = new Multimap();
      }
      get started() {
          return this.elementObserver.started;
      }
      get selector() {
          return this._selector;
      }
      set selector(selector) {
          this._selector = selector;
          this.refresh();
      }
      start() {
          this.elementObserver.start();
      }
      pause(callback) {
          this.elementObserver.pause(callback);
      }
      stop() {
          this.elementObserver.stop();
      }
      refresh() {
          this.elementObserver.refresh();
      }
      get element() {
          return this.elementObserver.element;
      }
      matchElement(element) {
          const { selector } = this;
          if (selector) {
              const matches = element.matches(selector);
              if (this.delegate.selectorMatchElement) {
                  return matches && this.delegate.selectorMatchElement(element, this.details);
              }
              return matches;
          }
          else {
              return false;
          }
      }
      matchElementsInTree(tree) {
          const { selector } = this;
          if (selector) {
              const match = this.matchElement(tree) ? [tree] : [];
              const matches = Array.from(tree.querySelectorAll(selector)).filter((match) => this.matchElement(match));
              return match.concat(matches);
          }
          else {
              return [];
          }
      }
      elementMatched(element) {
          const { selector } = this;
          if (selector) {
              this.selectorMatched(element, selector);
          }
      }
      elementUnmatched(element) {
          const selectors = this.matchesByElement.getKeysForValue(element);
          for (const selector of selectors) {
              this.selectorUnmatched(element, selector);
          }
      }
      elementAttributeChanged(element, _attributeName) {
          const { selector } = this;
          if (selector) {
              const matches = this.matchElement(element);
              const matchedBefore = this.matchesByElement.has(selector, element);
              if (matches && !matchedBefore) {
                  this.selectorMatched(element, selector);
              }
              else if (!matches && matchedBefore) {
                  this.selectorUnmatched(element, selector);
              }
          }
      }
      selectorMatched(element, selector) {
          this.delegate.selectorMatched(element, selector, this.details);
          this.matchesByElement.add(selector, element);
      }
      selectorUnmatched(element, selector) {
          this.delegate.selectorUnmatched(element, selector, this.details);
          this.matchesByElement.delete(selector, element);
      }
  }

  class StringMapObserver {
      constructor(element, delegate) {
          this.element = element;
          this.delegate = delegate;
          this.started = false;
          this.stringMap = new Map();
          this.mutationObserver = new MutationObserver((mutations) => this.processMutations(mutations));
      }
      start() {
          if (!this.started) {
              this.started = true;
              this.mutationObserver.observe(this.element, { attributes: true, attributeOldValue: true });
              this.refresh();
          }
      }
      stop() {
          if (this.started) {
              this.mutationObserver.takeRecords();
              this.mutationObserver.disconnect();
              this.started = false;
          }
      }
      refresh() {
          if (this.started) {
              for (const attributeName of this.knownAttributeNames) {
                  this.refreshAttribute(attributeName, null);
              }
          }
      }
      processMutations(mutations) {
          if (this.started) {
              for (const mutation of mutations) {
                  this.processMutation(mutation);
              }
          }
      }
      processMutation(mutation) {
          const attributeName = mutation.attributeName;
          if (attributeName) {
              this.refreshAttribute(attributeName, mutation.oldValue);
          }
      }
      refreshAttribute(attributeName, oldValue) {
          const key = this.delegate.getStringMapKeyForAttribute(attributeName);
          if (key != null) {
              if (!this.stringMap.has(attributeName)) {
                  this.stringMapKeyAdded(key, attributeName);
              }
              const value = this.element.getAttribute(attributeName);
              if (this.stringMap.get(attributeName) != value) {
                  this.stringMapValueChanged(value, key, oldValue);
              }
              if (value == null) {
                  const oldValue = this.stringMap.get(attributeName);
                  this.stringMap.delete(attributeName);
                  if (oldValue)
                      this.stringMapKeyRemoved(key, attributeName, oldValue);
              }
              else {
                  this.stringMap.set(attributeName, value);
              }
          }
      }
      stringMapKeyAdded(key, attributeName) {
          if (this.delegate.stringMapKeyAdded) {
              this.delegate.stringMapKeyAdded(key, attributeName);
          }
      }
      stringMapValueChanged(value, key, oldValue) {
          if (this.delegate.stringMapValueChanged) {
              this.delegate.stringMapValueChanged(value, key, oldValue);
          }
      }
      stringMapKeyRemoved(key, attributeName, oldValue) {
          if (this.delegate.stringMapKeyRemoved) {
              this.delegate.stringMapKeyRemoved(key, attributeName, oldValue);
          }
      }
      get knownAttributeNames() {
          return Array.from(new Set(this.currentAttributeNames.concat(this.recordedAttributeNames)));
      }
      get currentAttributeNames() {
          return Array.from(this.element.attributes).map((attribute) => attribute.name);
      }
      get recordedAttributeNames() {
          return Array.from(this.stringMap.keys());
      }
  }

  class TokenListObserver {
      constructor(element, attributeName, delegate) {
          this.attributeObserver = new AttributeObserver(element, attributeName, this);
          this.delegate = delegate;
          this.tokensByElement = new Multimap();
      }
      get started() {
          return this.attributeObserver.started;
      }
      start() {
          this.attributeObserver.start();
      }
      pause(callback) {
          this.attributeObserver.pause(callback);
      }
      stop() {
          this.attributeObserver.stop();
      }
      refresh() {
          this.attributeObserver.refresh();
      }
      get element() {
          return this.attributeObserver.element;
      }
      get attributeName() {
          return this.attributeObserver.attributeName;
      }
      elementMatchedAttribute(element) {
          this.tokensMatched(this.readTokensForElement(element));
      }
      elementAttributeValueChanged(element) {
          const [unmatchedTokens, matchedTokens] = this.refreshTokensForElement(element);
          this.tokensUnmatched(unmatchedTokens);
          this.tokensMatched(matchedTokens);
      }
      elementUnmatchedAttribute(element) {
          this.tokensUnmatched(this.tokensByElement.getValuesForKey(element));
      }
      tokensMatched(tokens) {
          tokens.forEach((token) => this.tokenMatched(token));
      }
      tokensUnmatched(tokens) {
          tokens.forEach((token) => this.tokenUnmatched(token));
      }
      tokenMatched(token) {
          this.delegate.tokenMatched(token);
          this.tokensByElement.add(token.element, token);
      }
      tokenUnmatched(token) {
          this.delegate.tokenUnmatched(token);
          this.tokensByElement.delete(token.element, token);
      }
      refreshTokensForElement(element) {
          const previousTokens = this.tokensByElement.getValuesForKey(element);
          const currentTokens = this.readTokensForElement(element);
          const firstDifferingIndex = zip(previousTokens, currentTokens).findIndex(([previousToken, currentToken]) => !tokensAreEqual(previousToken, currentToken));
          if (firstDifferingIndex == -1) {
              return [[], []];
          }
          else {
              return [previousTokens.slice(firstDifferingIndex), currentTokens.slice(firstDifferingIndex)];
          }
      }
      readTokensForElement(element) {
          const attributeName = this.attributeName;
          const tokenString = element.getAttribute(attributeName) || "";
          return parseTokenString(tokenString, element, attributeName);
      }
  }
  function parseTokenString(tokenString, element, attributeName) {
      return tokenString
          .trim()
          .split(/\s+/)
          .filter((content) => content.length)
          .map((content, index) => ({ element, attributeName, content, index }));
  }
  function zip(left, right) {
      const length = Math.max(left.length, right.length);
      return Array.from({ length }, (_, index) => [left[index], right[index]]);
  }
  function tokensAreEqual(left, right) {
      return left && right && left.index == right.index && left.content == right.content;
  }

  class ValueListObserver {
      constructor(element, attributeName, delegate) {
          this.tokenListObserver = new TokenListObserver(element, attributeName, this);
          this.delegate = delegate;
          this.parseResultsByToken = new WeakMap();
          this.valuesByTokenByElement = new WeakMap();
      }
      get started() {
          return this.tokenListObserver.started;
      }
      start() {
          this.tokenListObserver.start();
      }
      stop() {
          this.tokenListObserver.stop();
      }
      refresh() {
          this.tokenListObserver.refresh();
      }
      get element() {
          return this.tokenListObserver.element;
      }
      get attributeName() {
          return this.tokenListObserver.attributeName;
      }
      tokenMatched(token) {
          const { element } = token;
          const { value } = this.fetchParseResultForToken(token);
          if (value) {
              this.fetchValuesByTokenForElement(element).set(token, value);
              this.delegate.elementMatchedValue(element, value);
          }
      }
      tokenUnmatched(token) {
          const { element } = token;
          const { value } = this.fetchParseResultForToken(token);
          if (value) {
              this.fetchValuesByTokenForElement(element).delete(token);
              this.delegate.elementUnmatchedValue(element, value);
          }
      }
      fetchParseResultForToken(token) {
          let parseResult = this.parseResultsByToken.get(token);
          if (!parseResult) {
              parseResult = this.parseToken(token);
              this.parseResultsByToken.set(token, parseResult);
          }
          return parseResult;
      }
      fetchValuesByTokenForElement(element) {
          let valuesByToken = this.valuesByTokenByElement.get(element);
          if (!valuesByToken) {
              valuesByToken = new Map();
              this.valuesByTokenByElement.set(element, valuesByToken);
          }
          return valuesByToken;
      }
      parseToken(token) {
          try {
              const value = this.delegate.parseValueForToken(token);
              return { value };
          }
          catch (error) {
              return { error };
          }
      }
  }

  class BindingObserver {
      constructor(context, delegate) {
          this.context = context;
          this.delegate = delegate;
          this.bindingsByAction = new Map();
      }
      start() {
          if (!this.valueListObserver) {
              this.valueListObserver = new ValueListObserver(this.element, this.actionAttribute, this);
              this.valueListObserver.start();
          }
      }
      stop() {
          if (this.valueListObserver) {
              this.valueListObserver.stop();
              delete this.valueListObserver;
              this.disconnectAllActions();
          }
      }
      get element() {
          return this.context.element;
      }
      get identifier() {
          return this.context.identifier;
      }
      get actionAttribute() {
          return this.schema.actionAttribute;
      }
      get schema() {
          return this.context.schema;
      }
      get bindings() {
          return Array.from(this.bindingsByAction.values());
      }
      connectAction(action) {
          const binding = new Binding(this.context, action);
          this.bindingsByAction.set(action, binding);
          this.delegate.bindingConnected(binding);
      }
      disconnectAction(action) {
          const binding = this.bindingsByAction.get(action);
          if (binding) {
              this.bindingsByAction.delete(action);
              this.delegate.bindingDisconnected(binding);
          }
      }
      disconnectAllActions() {
          this.bindings.forEach((binding) => this.delegate.bindingDisconnected(binding, true));
          this.bindingsByAction.clear();
      }
      parseValueForToken(token) {
          const action = Action.forToken(token, this.schema);
          if (action.identifier == this.identifier) {
              return action;
          }
      }
      elementMatchedValue(element, action) {
          this.connectAction(action);
      }
      elementUnmatchedValue(element, action) {
          this.disconnectAction(action);
      }
  }

  class ValueObserver {
      constructor(context, receiver) {
          this.context = context;
          this.receiver = receiver;
          this.stringMapObserver = new StringMapObserver(this.element, this);
          this.valueDescriptorMap = this.controller.valueDescriptorMap;
      }
      start() {
          this.stringMapObserver.start();
          this.invokeChangedCallbacksForDefaultValues();
      }
      stop() {
          this.stringMapObserver.stop();
      }
      get element() {
          return this.context.element;
      }
      get controller() {
          return this.context.controller;
      }
      getStringMapKeyForAttribute(attributeName) {
          if (attributeName in this.valueDescriptorMap) {
              return this.valueDescriptorMap[attributeName].name;
          }
      }
      stringMapKeyAdded(key, attributeName) {
          const descriptor = this.valueDescriptorMap[attributeName];
          if (!this.hasValue(key)) {
              this.invokeChangedCallback(key, descriptor.writer(this.receiver[key]), descriptor.writer(descriptor.defaultValue));
          }
      }
      stringMapValueChanged(value, name, oldValue) {
          const descriptor = this.valueDescriptorNameMap[name];
          if (value === null)
              return;
          if (oldValue === null) {
              oldValue = descriptor.writer(descriptor.defaultValue);
          }
          this.invokeChangedCallback(name, value, oldValue);
      }
      stringMapKeyRemoved(key, attributeName, oldValue) {
          const descriptor = this.valueDescriptorNameMap[key];
          if (this.hasValue(key)) {
              this.invokeChangedCallback(key, descriptor.writer(this.receiver[key]), oldValue);
          }
          else {
              this.invokeChangedCallback(key, descriptor.writer(descriptor.defaultValue), oldValue);
          }
      }
      invokeChangedCallbacksForDefaultValues() {
          for (const { key, name, defaultValue, writer } of this.valueDescriptors) {
              if (defaultValue != undefined && !this.controller.data.has(key)) {
                  this.invokeChangedCallback(name, writer(defaultValue), undefined);
              }
          }
      }
      invokeChangedCallback(name, rawValue, rawOldValue) {
          const changedMethodName = `${name}Changed`;
          const changedMethod = this.receiver[changedMethodName];
          if (typeof changedMethod == "function") {
              const descriptor = this.valueDescriptorNameMap[name];
              try {
                  const value = descriptor.reader(rawValue);
                  let oldValue = rawOldValue;
                  if (rawOldValue) {
                      oldValue = descriptor.reader(rawOldValue);
                  }
                  changedMethod.call(this.receiver, value, oldValue);
              }
              catch (error) {
                  if (error instanceof TypeError) {
                      error.message = `Stimulus Value "${this.context.identifier}.${descriptor.name}" - ${error.message}`;
                  }
                  throw error;
              }
          }
      }
      get valueDescriptors() {
          const { valueDescriptorMap } = this;
          return Object.keys(valueDescriptorMap).map((key) => valueDescriptorMap[key]);
      }
      get valueDescriptorNameMap() {
          const descriptors = {};
          Object.keys(this.valueDescriptorMap).forEach((key) => {
              const descriptor = this.valueDescriptorMap[key];
              descriptors[descriptor.name] = descriptor;
          });
          return descriptors;
      }
      hasValue(attributeName) {
          const descriptor = this.valueDescriptorNameMap[attributeName];
          const hasMethodName = `has${capitalize(descriptor.name)}`;
          return this.receiver[hasMethodName];
      }
  }

  class TargetObserver {
      constructor(context, delegate) {
          this.context = context;
          this.delegate = delegate;
          this.targetsByName = new Multimap();
      }
      start() {
          if (!this.tokenListObserver) {
              this.tokenListObserver = new TokenListObserver(this.element, this.attributeName, this);
              this.tokenListObserver.start();
          }
      }
      stop() {
          if (this.tokenListObserver) {
              this.disconnectAllTargets();
              this.tokenListObserver.stop();
              delete this.tokenListObserver;
          }
      }
      tokenMatched({ element, content: name }) {
          if (this.scope.containsElement(element)) {
              this.connectTarget(element, name);
          }
      }
      tokenUnmatched({ element, content: name }) {
          this.disconnectTarget(element, name);
      }
      connectTarget(element, name) {
          var _a;
          if (!this.targetsByName.has(name, element)) {
              this.targetsByName.add(name, element);
              (_a = this.tokenListObserver) === null || _a === void 0 ? void 0 : _a.pause(() => this.delegate.targetConnected(element, name));
          }
      }
      disconnectTarget(element, name) {
          var _a;
          if (this.targetsByName.has(name, element)) {
              this.targetsByName.delete(name, element);
              (_a = this.tokenListObserver) === null || _a === void 0 ? void 0 : _a.pause(() => this.delegate.targetDisconnected(element, name));
          }
      }
      disconnectAllTargets() {
          for (const name of this.targetsByName.keys) {
              for (const element of this.targetsByName.getValuesForKey(name)) {
                  this.disconnectTarget(element, name);
              }
          }
      }
      get attributeName() {
          return `data-${this.context.identifier}-target`;
      }
      get element() {
          return this.context.element;
      }
      get scope() {
          return this.context.scope;
      }
  }

  function readInheritableStaticArrayValues(constructor, propertyName) {
      const ancestors = getAncestorsForConstructor(constructor);
      return Array.from(ancestors.reduce((values, constructor) => {
          getOwnStaticArrayValues(constructor, propertyName).forEach((name) => values.add(name));
          return values;
      }, new Set()));
  }
  function getAncestorsForConstructor(constructor) {
      const ancestors = [];
      while (constructor) {
          ancestors.push(constructor);
          constructor = Object.getPrototypeOf(constructor);
      }
      return ancestors.reverse();
  }
  function getOwnStaticArrayValues(constructor, propertyName) {
      const definition = constructor[propertyName];
      return Array.isArray(definition) ? definition : [];
  }

  class OutletObserver {
      constructor(context, delegate) {
          this.started = false;
          this.context = context;
          this.delegate = delegate;
          this.outletsByName = new Multimap();
          this.outletElementsByName = new Multimap();
          this.selectorObserverMap = new Map();
          this.attributeObserverMap = new Map();
      }
      start() {
          if (!this.started) {
              this.outletDefinitions.forEach((outletName) => {
                  this.setupSelectorObserverForOutlet(outletName);
                  this.setupAttributeObserverForOutlet(outletName);
              });
              this.started = true;
              this.dependentContexts.forEach((context) => context.refresh());
          }
      }
      refresh() {
          this.selectorObserverMap.forEach((observer) => observer.refresh());
          this.attributeObserverMap.forEach((observer) => observer.refresh());
      }
      stop() {
          if (this.started) {
              this.started = false;
              this.disconnectAllOutlets();
              this.stopSelectorObservers();
              this.stopAttributeObservers();
          }
      }
      stopSelectorObservers() {
          if (this.selectorObserverMap.size > 0) {
              this.selectorObserverMap.forEach((observer) => observer.stop());
              this.selectorObserverMap.clear();
          }
      }
      stopAttributeObservers() {
          if (this.attributeObserverMap.size > 0) {
              this.attributeObserverMap.forEach((observer) => observer.stop());
              this.attributeObserverMap.clear();
          }
      }
      selectorMatched(element, _selector, { outletName }) {
          const outlet = this.getOutlet(element, outletName);
          if (outlet) {
              this.connectOutlet(outlet, element, outletName);
          }
      }
      selectorUnmatched(element, _selector, { outletName }) {
          const outlet = this.getOutletFromMap(element, outletName);
          if (outlet) {
              this.disconnectOutlet(outlet, element, outletName);
          }
      }
      selectorMatchElement(element, { outletName }) {
          const selector = this.selector(outletName);
          const hasOutlet = this.hasOutlet(element, outletName);
          const hasOutletController = element.matches(`[${this.schema.controllerAttribute}~=${outletName}]`);
          if (selector) {
              return hasOutlet && hasOutletController && element.matches(selector);
          }
          else {
              return false;
          }
      }
      elementMatchedAttribute(_element, attributeName) {
          const outletName = this.getOutletNameFromOutletAttributeName(attributeName);
          if (outletName) {
              this.updateSelectorObserverForOutlet(outletName);
          }
      }
      elementAttributeValueChanged(_element, attributeName) {
          const outletName = this.getOutletNameFromOutletAttributeName(attributeName);
          if (outletName) {
              this.updateSelectorObserverForOutlet(outletName);
          }
      }
      elementUnmatchedAttribute(_element, attributeName) {
          const outletName = this.getOutletNameFromOutletAttributeName(attributeName);
          if (outletName) {
              this.updateSelectorObserverForOutlet(outletName);
          }
      }
      connectOutlet(outlet, element, outletName) {
          var _a;
          if (!this.outletElementsByName.has(outletName, element)) {
              this.outletsByName.add(outletName, outlet);
              this.outletElementsByName.add(outletName, element);
              (_a = this.selectorObserverMap.get(outletName)) === null || _a === void 0 ? void 0 : _a.pause(() => this.delegate.outletConnected(outlet, element, outletName));
          }
      }
      disconnectOutlet(outlet, element, outletName) {
          var _a;
          if (this.outletElementsByName.has(outletName, element)) {
              this.outletsByName.delete(outletName, outlet);
              this.outletElementsByName.delete(outletName, element);
              (_a = this.selectorObserverMap
                  .get(outletName)) === null || _a === void 0 ? void 0 : _a.pause(() => this.delegate.outletDisconnected(outlet, element, outletName));
          }
      }
      disconnectAllOutlets() {
          for (const outletName of this.outletElementsByName.keys) {
              for (const element of this.outletElementsByName.getValuesForKey(outletName)) {
                  for (const outlet of this.outletsByName.getValuesForKey(outletName)) {
                      this.disconnectOutlet(outlet, element, outletName);
                  }
              }
          }
      }
      updateSelectorObserverForOutlet(outletName) {
          const observer = this.selectorObserverMap.get(outletName);
          if (observer) {
              observer.selector = this.selector(outletName);
          }
      }
      setupSelectorObserverForOutlet(outletName) {
          const selector = this.selector(outletName);
          const selectorObserver = new SelectorObserver(document.body, selector, this, { outletName });
          this.selectorObserverMap.set(outletName, selectorObserver);
          selectorObserver.start();
      }
      setupAttributeObserverForOutlet(outletName) {
          const attributeName = this.attributeNameForOutletName(outletName);
          const attributeObserver = new AttributeObserver(this.scope.element, attributeName, this);
          this.attributeObserverMap.set(outletName, attributeObserver);
          attributeObserver.start();
      }
      selector(outletName) {
          return this.scope.outlets.getSelectorForOutletName(outletName);
      }
      attributeNameForOutletName(outletName) {
          return this.scope.schema.outletAttributeForScope(this.identifier, outletName);
      }
      getOutletNameFromOutletAttributeName(attributeName) {
          return this.outletDefinitions.find((outletName) => this.attributeNameForOutletName(outletName) === attributeName);
      }
      get outletDependencies() {
          const dependencies = new Multimap();
          this.router.modules.forEach((module) => {
              const constructor = module.definition.controllerConstructor;
              const outlets = readInheritableStaticArrayValues(constructor, "outlets");
              outlets.forEach((outlet) => dependencies.add(outlet, module.identifier));
          });
          return dependencies;
      }
      get outletDefinitions() {
          return this.outletDependencies.getKeysForValue(this.identifier);
      }
      get dependentControllerIdentifiers() {
          return this.outletDependencies.getValuesForKey(this.identifier);
      }
      get dependentContexts() {
          const identifiers = this.dependentControllerIdentifiers;
          return this.router.contexts.filter((context) => identifiers.includes(context.identifier));
      }
      hasOutlet(element, outletName) {
          return !!this.getOutlet(element, outletName) || !!this.getOutletFromMap(element, outletName);
      }
      getOutlet(element, outletName) {
          return this.application.getControllerForElementAndIdentifier(element, outletName);
      }
      getOutletFromMap(element, outletName) {
          return this.outletsByName.getValuesForKey(outletName).find((outlet) => outlet.element === element);
      }
      get scope() {
          return this.context.scope;
      }
      get schema() {
          return this.context.schema;
      }
      get identifier() {
          return this.context.identifier;
      }
      get application() {
          return this.context.application;
      }
      get router() {
          return this.application.router;
      }
  }

  class Context {
      constructor(module, scope) {
          this.logDebugActivity = (functionName, detail = {}) => {
              const { identifier, controller, element } = this;
              detail = Object.assign({ identifier, controller, element }, detail);
              this.application.logDebugActivity(this.identifier, functionName, detail);
          };
          this.module = module;
          this.scope = scope;
          this.controller = new module.controllerConstructor(this);
          this.bindingObserver = new BindingObserver(this, this.dispatcher);
          this.valueObserver = new ValueObserver(this, this.controller);
          this.targetObserver = new TargetObserver(this, this);
          this.outletObserver = new OutletObserver(this, this);
          try {
              this.controller.initialize();
              this.logDebugActivity("initialize");
          }
          catch (error) {
              this.handleError(error, "initializing controller");
          }
      }
      connect() {
          this.bindingObserver.start();
          this.valueObserver.start();
          this.targetObserver.start();
          this.outletObserver.start();
          try {
              this.controller.connect();
              this.logDebugActivity("connect");
          }
          catch (error) {
              this.handleError(error, "connecting controller");
          }
      }
      refresh() {
          this.outletObserver.refresh();
      }
      disconnect() {
          try {
              this.controller.disconnect();
              this.logDebugActivity("disconnect");
          }
          catch (error) {
              this.handleError(error, "disconnecting controller");
          }
          this.outletObserver.stop();
          this.targetObserver.stop();
          this.valueObserver.stop();
          this.bindingObserver.stop();
      }
      get application() {
          return this.module.application;
      }
      get identifier() {
          return this.module.identifier;
      }
      get schema() {
          return this.application.schema;
      }
      get dispatcher() {
          return this.application.dispatcher;
      }
      get element() {
          return this.scope.element;
      }
      get parentElement() {
          return this.element.parentElement;
      }
      handleError(error, message, detail = {}) {
          const { identifier, controller, element } = this;
          detail = Object.assign({ identifier, controller, element }, detail);
          this.application.handleError(error, `Error ${message}`, detail);
      }
      targetConnected(element, name) {
          this.invokeControllerMethod(`${name}TargetConnected`, element);
      }
      targetDisconnected(element, name) {
          this.invokeControllerMethod(`${name}TargetDisconnected`, element);
      }
      outletConnected(outlet, element, name) {
          this.invokeControllerMethod(`${namespaceCamelize(name)}OutletConnected`, outlet, element);
      }
      outletDisconnected(outlet, element, name) {
          this.invokeControllerMethod(`${namespaceCamelize(name)}OutletDisconnected`, outlet, element);
      }
      invokeControllerMethod(methodName, ...args) {
          const controller = this.controller;
          if (typeof controller[methodName] == "function") {
              controller[methodName](...args);
          }
      }
  }

  function bless(constructor) {
      return shadow(constructor, getBlessedProperties(constructor));
  }
  function shadow(constructor, properties) {
      const shadowConstructor = extend(constructor);
      const shadowProperties = getShadowProperties(constructor.prototype, properties);
      Object.defineProperties(shadowConstructor.prototype, shadowProperties);
      return shadowConstructor;
  }
  function getBlessedProperties(constructor) {
      const blessings = readInheritableStaticArrayValues(constructor, "blessings");
      return blessings.reduce((blessedProperties, blessing) => {
          const properties = blessing(constructor);
          for (const key in properties) {
              const descriptor = blessedProperties[key] || {};
              blessedProperties[key] = Object.assign(descriptor, properties[key]);
          }
          return blessedProperties;
      }, {});
  }
  function getShadowProperties(prototype, properties) {
      return getOwnKeys(properties).reduce((shadowProperties, key) => {
          const descriptor = getShadowedDescriptor(prototype, properties, key);
          if (descriptor) {
              Object.assign(shadowProperties, { [key]: descriptor });
          }
          return shadowProperties;
      }, {});
  }
  function getShadowedDescriptor(prototype, properties, key) {
      const shadowingDescriptor = Object.getOwnPropertyDescriptor(prototype, key);
      const shadowedByValue = shadowingDescriptor && "value" in shadowingDescriptor;
      if (!shadowedByValue) {
          const descriptor = Object.getOwnPropertyDescriptor(properties, key).value;
          if (shadowingDescriptor) {
              descriptor.get = shadowingDescriptor.get || descriptor.get;
              descriptor.set = shadowingDescriptor.set || descriptor.set;
          }
          return descriptor;
      }
  }
  const getOwnKeys = (() => {
      if (typeof Object.getOwnPropertySymbols == "function") {
          return (object) => [...Object.getOwnPropertyNames(object), ...Object.getOwnPropertySymbols(object)];
      }
      else {
          return Object.getOwnPropertyNames;
      }
  })();
  const extend = (() => {
      function extendWithReflect(constructor) {
          function extended() {
              return Reflect.construct(constructor, arguments, new.target);
          }
          extended.prototype = Object.create(constructor.prototype, {
              constructor: { value: extended },
          });
          Reflect.setPrototypeOf(extended, constructor);
          return extended;
      }
      function testReflectExtension() {
          const a = function () {
              this.a.call(this);
          };
          const b = extendWithReflect(a);
          b.prototype.a = function () { };
          return new b();
      }
      try {
          testReflectExtension();
          return extendWithReflect;
      }
      catch (error) {
          return (constructor) => class extended extends constructor {
          };
      }
  })();

  function blessDefinition(definition) {
      return {
          identifier: definition.identifier,
          controllerConstructor: bless(definition.controllerConstructor),
      };
  }

  class Module {
      constructor(application, definition) {
          this.application = application;
          this.definition = blessDefinition(definition);
          this.contextsByScope = new WeakMap();
          this.connectedContexts = new Set();
      }
      get identifier() {
          return this.definition.identifier;
      }
      get controllerConstructor() {
          return this.definition.controllerConstructor;
      }
      get contexts() {
          return Array.from(this.connectedContexts);
      }
      connectContextForScope(scope) {
          const context = this.fetchContextForScope(scope);
          this.connectedContexts.add(context);
          context.connect();
      }
      disconnectContextForScope(scope) {
          const context = this.contextsByScope.get(scope);
          if (context) {
              this.connectedContexts.delete(context);
              context.disconnect();
          }
      }
      fetchContextForScope(scope) {
          let context = this.contextsByScope.get(scope);
          if (!context) {
              context = new Context(this, scope);
              this.contextsByScope.set(scope, context);
          }
          return context;
      }
  }

  class ClassMap {
      constructor(scope) {
          this.scope = scope;
      }
      has(name) {
          return this.data.has(this.getDataKey(name));
      }
      get(name) {
          return this.getAll(name)[0];
      }
      getAll(name) {
          const tokenString = this.data.get(this.getDataKey(name)) || "";
          return tokenize(tokenString);
      }
      getAttributeName(name) {
          return this.data.getAttributeNameForKey(this.getDataKey(name));
      }
      getDataKey(name) {
          return `${name}-class`;
      }
      get data() {
          return this.scope.data;
      }
  }

  class DataMap {
      constructor(scope) {
          this.scope = scope;
      }
      get element() {
          return this.scope.element;
      }
      get identifier() {
          return this.scope.identifier;
      }
      get(key) {
          const name = this.getAttributeNameForKey(key);
          return this.element.getAttribute(name);
      }
      set(key, value) {
          const name = this.getAttributeNameForKey(key);
          this.element.setAttribute(name, value);
          return this.get(key);
      }
      has(key) {
          const name = this.getAttributeNameForKey(key);
          return this.element.hasAttribute(name);
      }
      delete(key) {
          if (this.has(key)) {
              const name = this.getAttributeNameForKey(key);
              this.element.removeAttribute(name);
              return true;
          }
          else {
              return false;
          }
      }
      getAttributeNameForKey(key) {
          return `data-${this.identifier}-${dasherize(key)}`;
      }
  }

  class Guide {
      constructor(logger) {
          this.warnedKeysByObject = new WeakMap();
          this.logger = logger;
      }
      warn(object, key, message) {
          let warnedKeys = this.warnedKeysByObject.get(object);
          if (!warnedKeys) {
              warnedKeys = new Set();
              this.warnedKeysByObject.set(object, warnedKeys);
          }
          if (!warnedKeys.has(key)) {
              warnedKeys.add(key);
              this.logger.warn(message, object);
          }
      }
  }

  function attributeValueContainsToken(attributeName, token) {
      return `[${attributeName}~="${token}"]`;
  }

  class TargetSet {
      constructor(scope) {
          this.scope = scope;
      }
      get element() {
          return this.scope.element;
      }
      get identifier() {
          return this.scope.identifier;
      }
      get schema() {
          return this.scope.schema;
      }
      has(targetName) {
          return this.find(targetName) != null;
      }
      find(...targetNames) {
          return targetNames.reduce((target, targetName) => target || this.findTarget(targetName) || this.findLegacyTarget(targetName), undefined);
      }
      findAll(...targetNames) {
          return targetNames.reduce((targets, targetName) => [
              ...targets,
              ...this.findAllTargets(targetName),
              ...this.findAllLegacyTargets(targetName),
          ], []);
      }
      findTarget(targetName) {
          const selector = this.getSelectorForTargetName(targetName);
          return this.scope.findElement(selector);
      }
      findAllTargets(targetName) {
          const selector = this.getSelectorForTargetName(targetName);
          return this.scope.findAllElements(selector);
      }
      getSelectorForTargetName(targetName) {
          const attributeName = this.schema.targetAttributeForScope(this.identifier);
          return attributeValueContainsToken(attributeName, targetName);
      }
      findLegacyTarget(targetName) {
          const selector = this.getLegacySelectorForTargetName(targetName);
          return this.deprecate(this.scope.findElement(selector), targetName);
      }
      findAllLegacyTargets(targetName) {
          const selector = this.getLegacySelectorForTargetName(targetName);
          return this.scope.findAllElements(selector).map((element) => this.deprecate(element, targetName));
      }
      getLegacySelectorForTargetName(targetName) {
          const targetDescriptor = `${this.identifier}.${targetName}`;
          return attributeValueContainsToken(this.schema.targetAttribute, targetDescriptor);
      }
      deprecate(element, targetName) {
          if (element) {
              const { identifier } = this;
              const attributeName = this.schema.targetAttribute;
              const revisedAttributeName = this.schema.targetAttributeForScope(identifier);
              this.guide.warn(element, `target:${targetName}`, `Please replace ${attributeName}="${identifier}.${targetName}" with ${revisedAttributeName}="${targetName}". ` +
                  `The ${attributeName} attribute is deprecated and will be removed in a future version of Stimulus.`);
          }
          return element;
      }
      get guide() {
          return this.scope.guide;
      }
  }

  class OutletSet {
      constructor(scope, controllerElement) {
          this.scope = scope;
          this.controllerElement = controllerElement;
      }
      get element() {
          return this.scope.element;
      }
      get identifier() {
          return this.scope.identifier;
      }
      get schema() {
          return this.scope.schema;
      }
      has(outletName) {
          return this.find(outletName) != null;
      }
      find(...outletNames) {
          return outletNames.reduce((outlet, outletName) => outlet || this.findOutlet(outletName), undefined);
      }
      findAll(...outletNames) {
          return outletNames.reduce((outlets, outletName) => [...outlets, ...this.findAllOutlets(outletName)], []);
      }
      getSelectorForOutletName(outletName) {
          const attributeName = this.schema.outletAttributeForScope(this.identifier, outletName);
          return this.controllerElement.getAttribute(attributeName);
      }
      findOutlet(outletName) {
          const selector = this.getSelectorForOutletName(outletName);
          if (selector)
              return this.findElement(selector, outletName);
      }
      findAllOutlets(outletName) {
          const selector = this.getSelectorForOutletName(outletName);
          return selector ? this.findAllElements(selector, outletName) : [];
      }
      findElement(selector, outletName) {
          const elements = this.scope.queryElements(selector);
          return elements.filter((element) => this.matchesElement(element, selector, outletName))[0];
      }
      findAllElements(selector, outletName) {
          const elements = this.scope.queryElements(selector);
          return elements.filter((element) => this.matchesElement(element, selector, outletName));
      }
      matchesElement(element, selector, outletName) {
          const controllerAttribute = element.getAttribute(this.scope.schema.controllerAttribute) || "";
          return element.matches(selector) && controllerAttribute.split(" ").includes(outletName);
      }
  }

  class Scope {
      constructor(schema, element, identifier, logger) {
          this.targets = new TargetSet(this);
          this.classes = new ClassMap(this);
          this.data = new DataMap(this);
          this.containsElement = (element) => {
              return element.closest(this.controllerSelector) === this.element;
          };
          this.schema = schema;
          this.element = element;
          this.identifier = identifier;
          this.guide = new Guide(logger);
          this.outlets = new OutletSet(this.documentScope, element);
      }
      findElement(selector) {
          return this.element.matches(selector) ? this.element : this.queryElements(selector).find(this.containsElement);
      }
      findAllElements(selector) {
          return [
              ...(this.element.matches(selector) ? [this.element] : []),
              ...this.queryElements(selector).filter(this.containsElement),
          ];
      }
      queryElements(selector) {
          return Array.from(this.element.querySelectorAll(selector));
      }
      get controllerSelector() {
          return attributeValueContainsToken(this.schema.controllerAttribute, this.identifier);
      }
      get isDocumentScope() {
          return this.element === document.documentElement;
      }
      get documentScope() {
          return this.isDocumentScope
              ? this
              : new Scope(this.schema, document.documentElement, this.identifier, this.guide.logger);
      }
  }

  class ScopeObserver {
      constructor(element, schema, delegate) {
          this.element = element;
          this.schema = schema;
          this.delegate = delegate;
          this.valueListObserver = new ValueListObserver(this.element, this.controllerAttribute, this);
          this.scopesByIdentifierByElement = new WeakMap();
          this.scopeReferenceCounts = new WeakMap();
      }
      start() {
          this.valueListObserver.start();
      }
      stop() {
          this.valueListObserver.stop();
      }
      get controllerAttribute() {
          return this.schema.controllerAttribute;
      }
      parseValueForToken(token) {
          const { element, content: identifier } = token;
          return this.parseValueForElementAndIdentifier(element, identifier);
      }
      parseValueForElementAndIdentifier(element, identifier) {
          const scopesByIdentifier = this.fetchScopesByIdentifierForElement(element);
          let scope = scopesByIdentifier.get(identifier);
          if (!scope) {
              scope = this.delegate.createScopeForElementAndIdentifier(element, identifier);
              scopesByIdentifier.set(identifier, scope);
          }
          return scope;
      }
      elementMatchedValue(element, value) {
          const referenceCount = (this.scopeReferenceCounts.get(value) || 0) + 1;
          this.scopeReferenceCounts.set(value, referenceCount);
          if (referenceCount == 1) {
              this.delegate.scopeConnected(value);
          }
      }
      elementUnmatchedValue(element, value) {
          const referenceCount = this.scopeReferenceCounts.get(value);
          if (referenceCount) {
              this.scopeReferenceCounts.set(value, referenceCount - 1);
              if (referenceCount == 1) {
                  this.delegate.scopeDisconnected(value);
              }
          }
      }
      fetchScopesByIdentifierForElement(element) {
          let scopesByIdentifier = this.scopesByIdentifierByElement.get(element);
          if (!scopesByIdentifier) {
              scopesByIdentifier = new Map();
              this.scopesByIdentifierByElement.set(element, scopesByIdentifier);
          }
          return scopesByIdentifier;
      }
  }

  class Router {
      constructor(application) {
          this.application = application;
          this.scopeObserver = new ScopeObserver(this.element, this.schema, this);
          this.scopesByIdentifier = new Multimap();
          this.modulesByIdentifier = new Map();
      }
      get element() {
          return this.application.element;
      }
      get schema() {
          return this.application.schema;
      }
      get logger() {
          return this.application.logger;
      }
      get controllerAttribute() {
          return this.schema.controllerAttribute;
      }
      get modules() {
          return Array.from(this.modulesByIdentifier.values());
      }
      get contexts() {
          return this.modules.reduce((contexts, module) => contexts.concat(module.contexts), []);
      }
      start() {
          this.scopeObserver.start();
      }
      stop() {
          this.scopeObserver.stop();
      }
      loadDefinition(definition) {
          this.unloadIdentifier(definition.identifier);
          const module = new Module(this.application, definition);
          this.connectModule(module);
          const afterLoad = definition.controllerConstructor.afterLoad;
          if (afterLoad) {
              afterLoad.call(definition.controllerConstructor, definition.identifier, this.application);
          }
      }
      unloadIdentifier(identifier) {
          const module = this.modulesByIdentifier.get(identifier);
          if (module) {
              this.disconnectModule(module);
          }
      }
      getContextForElementAndIdentifier(element, identifier) {
          const module = this.modulesByIdentifier.get(identifier);
          if (module) {
              return module.contexts.find((context) => context.element == element);
          }
      }
      proposeToConnectScopeForElementAndIdentifier(element, identifier) {
          const scope = this.scopeObserver.parseValueForElementAndIdentifier(element, identifier);
          if (scope) {
              this.scopeObserver.elementMatchedValue(scope.element, scope);
          }
          else {
              console.error(`Couldn't find or create scope for identifier: "${identifier}" and element:`, element);
          }
      }
      handleError(error, message, detail) {
          this.application.handleError(error, message, detail);
      }
      createScopeForElementAndIdentifier(element, identifier) {
          return new Scope(this.schema, element, identifier, this.logger);
      }
      scopeConnected(scope) {
          this.scopesByIdentifier.add(scope.identifier, scope);
          const module = this.modulesByIdentifier.get(scope.identifier);
          if (module) {
              module.connectContextForScope(scope);
          }
      }
      scopeDisconnected(scope) {
          this.scopesByIdentifier.delete(scope.identifier, scope);
          const module = this.modulesByIdentifier.get(scope.identifier);
          if (module) {
              module.disconnectContextForScope(scope);
          }
      }
      connectModule(module) {
          this.modulesByIdentifier.set(module.identifier, module);
          const scopes = this.scopesByIdentifier.getValuesForKey(module.identifier);
          scopes.forEach((scope) => module.connectContextForScope(scope));
      }
      disconnectModule(module) {
          this.modulesByIdentifier.delete(module.identifier);
          const scopes = this.scopesByIdentifier.getValuesForKey(module.identifier);
          scopes.forEach((scope) => module.disconnectContextForScope(scope));
      }
  }

  const defaultSchema = {
      controllerAttribute: "data-controller",
      actionAttribute: "data-action",
      targetAttribute: "data-target",
      targetAttributeForScope: (identifier) => `data-${identifier}-target`,
      outletAttributeForScope: (identifier, outlet) => `data-${identifier}-${outlet}-outlet`,
      keyMappings: Object.assign(Object.assign({ enter: "Enter", tab: "Tab", esc: "Escape", space: " ", up: "ArrowUp", down: "ArrowDown", left: "ArrowLeft", right: "ArrowRight", home: "Home", end: "End", page_up: "PageUp", page_down: "PageDown" }, objectFromEntries("abcdefghijklmnopqrstuvwxyz".split("").map((c) => [c, c]))), objectFromEntries("0123456789".split("").map((n) => [n, n]))),
  };
  function objectFromEntries(array) {
      return array.reduce((memo, [k, v]) => (Object.assign(Object.assign({}, memo), { [k]: v })), {});
  }

  class Application {
      constructor(element = document.documentElement, schema = defaultSchema) {
          this.logger = console;
          this.debug = false;
          this.logDebugActivity = (identifier, functionName, detail = {}) => {
              if (this.debug) {
                  this.logFormattedMessage(identifier, functionName, detail);
              }
          };
          this.element = element;
          this.schema = schema;
          this.dispatcher = new Dispatcher(this);
          this.router = new Router(this);
          this.actionDescriptorFilters = Object.assign({}, defaultActionDescriptorFilters);
      }
      static start(element, schema) {
          const application = new this(element, schema);
          application.start();
          return application;
      }
      async start() {
          await domReady();
          this.logDebugActivity("application", "starting");
          this.dispatcher.start();
          this.router.start();
          this.logDebugActivity("application", "start");
      }
      stop() {
          this.logDebugActivity("application", "stopping");
          this.dispatcher.stop();
          this.router.stop();
          this.logDebugActivity("application", "stop");
      }
      register(identifier, controllerConstructor) {
          this.load({ identifier, controllerConstructor });
      }
      registerActionOption(name, filter) {
          this.actionDescriptorFilters[name] = filter;
      }
      load(head, ...rest) {
          const definitions = Array.isArray(head) ? head : [head, ...rest];
          definitions.forEach((definition) => {
              if (definition.controllerConstructor.shouldLoad) {
                  this.router.loadDefinition(definition);
              }
          });
      }
      unload(head, ...rest) {
          const identifiers = Array.isArray(head) ? head : [head, ...rest];
          identifiers.forEach((identifier) => this.router.unloadIdentifier(identifier));
      }
      get controllers() {
          return this.router.contexts.map((context) => context.controller);
      }
      getControllerForElementAndIdentifier(element, identifier) {
          const context = this.router.getContextForElementAndIdentifier(element, identifier);
          return context ? context.controller : null;
      }
      handleError(error, message, detail) {
          var _a;
          this.logger.error(`%s\n\n%o\n\n%o`, message, error, detail);
          (_a = window.onerror) === null || _a === void 0 ? void 0 : _a.call(window, message, "", 0, 0, error);
      }
      logFormattedMessage(identifier, functionName, detail = {}) {
          detail = Object.assign({ application: this }, detail);
          this.logger.groupCollapsed(`${identifier} #${functionName}`);
          this.logger.log("details:", Object.assign({}, detail));
          this.logger.groupEnd();
      }
  }
  function domReady() {
      return new Promise((resolve) => {
          if (document.readyState == "loading") {
              document.addEventListener("DOMContentLoaded", () => resolve());
          }
          else {
              resolve();
          }
      });
  }

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
      await this.#reloadChangedStimulusControllers();
      this.#unloadDeletedStimulusControllers();
      this.application.start();
    }
    async #reloadChangedStimulusControllers() {
      await Promise.all(this.#stimulusControllerPathsToReload.map(async moduleName => this.#reloadStimulusController(moduleName)));
    }
    get #stimulusControllerPathsToReload() {
      this.controllerPathsToReload = this.controllerPathsToReload || this.#stimulusControllerPaths.filter(path => this.#shouldReloadController(path));
      return this.controllerPathsToReload;
    }
    get #stimulusControllerPaths() {
      return Object.keys(this.#stimulusPathsByModule).filter(path => path.endsWith("_controller"));
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
    #unloadDeletedStimulusControllers() {
      this.#controllersToUnload.forEach(controller => this.#deregisterController(controller.identifier));
    }
    get #controllersToUnload() {
      if (this.#didChangeTriggerAReload) {
        return [];
      } else {
        return this.application.controllers.filter(controller => this.filePattern.test(`${controller.identifier}_controller`));
      }
    }
    get #didChangeTriggerAReload() {
      return this.#stimulusControllerPathsToReload.length > 0;
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
    #deregisterController(name) {
      log(`\tRemoving controller ${name}`);
      this.application.unload(name);
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
