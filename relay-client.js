/*
 * Egg Heist relay client.
 * Gives the game the same small "room" API it uses inside Claude artifacts,
 * backed by a WebSocket relay (see server/relay.js).
 *
 *   const lobby = await EggRelay.connect('wss://your-relay.example.com');
 *   lobby.presence({ srv: 0, nm: 'Ana' });
 *   lobby.onPeers(change => ...);          // change.peers = everyone in the lobby
 *   const room = await lobby.join('eggheist-s3');
 *   room.on('steal', msg => ...);          // msg = { topic, data, peer, isMe:false, ... }
 *   room.emit('steal', { to, i });
 */
(function (root) {
  'use strict';

  var MAX_FRAME = 8192;

  function connect(url, opts) {
    opts = opts || {};
    var WS = opts.WebSocket || root.WebSocket;
    return new Promise(function (resolve) {
      if (!WS) { resolve(null); return; }
      var ws = null, myId = null, resolved = false, tries = 0, timer = null, closedByUs = false;
      var rooms = new Map(); // name -> state
      var connectedFlag = false;

      function done(v) { if (!resolved) { resolved = true; resolve(v); } }
      function send(obj) {
        if (!ws || ws.readyState !== 1) return false;
        var s = JSON.stringify(obj);
        if (s.length > MAX_FRAME) return false;
        try { ws.send(s); return true; } catch (e) { return false; }
      }

      function makeState(name) {
        return { name: name, peers: Object.freeze([]), byId: new Map(), presence: {}, topics: new Map(), peerHandlers: new Set(), pendingJoin: null, left: false };
      }

      function peerObj(id, presence, prev) {
        var isMe = id === myId;
        if (prev && prev._json === JSON.stringify(presence) && prev.isMe === isMe) return prev;
        var o = { peer: id, by: null, isMe: isMe, sameTab: isMe, kind: 'viewer', guest: false, presence: Object.freeze(presence), updatedAt: Date.now() };
        Object.defineProperty(o, '_json', { value: JSON.stringify(presence), enumerable: false });
        return Object.freeze(o);
      }

      function applyPeers(st, list) {
        var next = new Map(), joined = [], updated = [], left = [], arr = [];
        for (var i = 0; i < list.length; i++) {
          var p = list[i];
          if (!p || typeof p.peer !== 'string') continue;
          var prev = st.byId.get(p.peer);
          var po = peerObj(p.peer, (p.presence && typeof p.presence === 'object') ? p.presence : {}, prev);
          next.set(p.peer, po); arr.push(po);
          if (!prev) joined.push(po); else if (prev !== po) updated.push(po);
        }
        st.byId.forEach(function (po, id) { if (!next.has(id)) left.push(po); });
        st.byId = next;
        st.peers = Object.freeze(arr);
        var change = { peers: st.peers, joined: joined, left: left, updated: updated };
        st.peerHandlers.forEach(function (fn) { try { fn(change); } catch (e) { /* keep going */ } });
        if (st.pendingJoin) { var pj = st.pendingJoin; st.pendingJoin = null; pj(); }
      }

      function api(st) {
        var o = {
          name: st.name,
          peers: function () { return st.peers; },
          onPeers: function (fn) {
            if (typeof fn !== 'function') throw new TypeError('handler must be a function');
            st.peerHandlers.add(fn);
            Promise.resolve().then(function () {
              if (!st.peerHandlers.has(fn)) return;
              fn({ peers: st.peers, joined: st.peers.slice(), left: [], updated: [] });
            });
            return function () { st.peerHandlers.delete(fn); };
          },
          presence: function (patch) {
            if (!patch || typeof patch !== 'object') return Promise.reject(new Error('invalid_argument'));
            for (var k in patch) {
              if (patch[k] === null) delete st.presence[k]; else st.presence[k] = patch[k];
            }
            send({ t: 'pres', room: st.name, patch: patch });
            return Promise.resolve();
          },
          emit: function (topic, data) {
            if (!/^[a-z][a-z0-9_.-]{0,47}$/.test(String(topic))) return Promise.reject(new Error('invalid_argument'));
            send({ t: 'emit', room: st.name, topic: topic, data: data });
            return Promise.resolve();
          },
          on: function (topic, fn) {
            if (typeof fn !== 'function') throw new TypeError('handler must be a function');
            var set = st.topics.get(topic);
            if (!set) { set = new Set(); st.topics.set(topic, set); }
            set.add(fn);
            return function () { set.delete(fn); };
          },
          connected: function () { return connectedFlag; }
        };
        return o;
      }

      var lobbyState = makeState('lobby');
      rooms.set('lobby', lobbyState);
      var lobby = api(lobbyState);

      lobby.join = function (name) {
        if (!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(String(name))) return Promise.reject(new Error('invalid_argument'));
        var existing = rooms.get(name);
        if (existing && !existing.left && existing.api) return Promise.resolve(existing.api);
        var st = makeState(name);
        rooms.set(name, st);
        var a = api(st);
        a.leave = function () {
          st.left = true; rooms.delete(name); st.peerHandlers.clear(); st.topics.clear();
          send({ t: 'leave', room: name });
          return Promise.resolve();
        };
        st.api = a;
        return new Promise(function (res, rej) {
          var to = setTimeout(function () { st.pendingJoin = null; rooms.delete(name); rej(new Error('upstream_error')); }, 10000);
          st.pendingJoin = function () { clearTimeout(to); res(a); };
          if (!send({ t: 'join', room: name })) { clearTimeout(to); rooms.delete(name); rej(new Error('upstream_error')); }
        });
      };

      function onMessage(raw) {
        var m;
        try { m = JSON.parse(raw); } catch (e) { return; }
        if (!m || typeof m !== 'object') return;
        if (m.t === 'hello' && typeof m.id === 'string') {
          myId = m.id; connectedFlag = true; tries = 0;
          // (re)join named rooms and re-assert presence after a reconnect
          rooms.forEach(function (st, name) {
            if (name !== 'lobby') send({ t: 'join', room: name });
            if (Object.keys(st.presence).length) send({ t: 'pres', room: name, patch: st.presence });
          });
          done(lobby);
        } else if (m.t === 'peers' && typeof m.room === 'string' && Array.isArray(m.peers)) {
          var st = rooms.get(m.room);
          if (st) applyPeers(st, m.peers);
        } else if (m.t === 'msg' && typeof m.room === 'string' && typeof m.topic === 'string') {
          var s2 = rooms.get(m.room);
          var set = s2 && s2.topics.get(m.topic);
          if (!set) return;
          var msg = { topic: m.topic, data: m.data, peer: String(m.peer || ''), by: null, isMe: false, sameTab: false, kind: 'viewer', guest: false };
          set.forEach(function (fn) { try { fn(msg); } catch (e) { /* keep going */ } });
        }
      }

      function open() {
        try { ws = new WS(url); } catch (e) { done(null); return; }
        ws.onopen = function () { /* wait for hello */ };
        ws.onmessage = function (ev) { onMessage(typeof ev.data === 'string' ? ev.data : ''); };
        ws.onerror = function () { /* close follows */ };
        ws.onclose = function () {
          connectedFlag = false;
          if (!resolved && tries >= 1) done(null);
          if (closedByUs) return;
          tries++;
          // everyone else disappears from our view until we reconnect
          rooms.forEach(function (st) { if (st.peers.length) applyPeers(st, []); });
          timer = setTimeout(open, Math.min(10000, 500 * Math.pow(2, Math.min(tries, 5))));
        };
      }

      open();
      setTimeout(function () { done(null); }, opts.timeoutMs || 6000);
      lobby.close = function () { closedByUs = true; clearTimeout(timer); try { ws && ws.close(); } catch (e) { /* ignore */ } };
    });
  }

  var api = { connect: connect };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.EggRelay = api;
})(typeof window !== 'undefined' ? window : globalThis);
