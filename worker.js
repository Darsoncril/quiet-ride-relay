// Ретранслятор комнат для «Тихой поездки» (Cloudflare Worker + Durable Object)
// Хозяин и гости соединяются по WebSocket, сервер только пересылает сообщения.

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{4})$/);
    if (!m) return new Response('Quiet Ride relay: ok');
    const id = env.ROOM.idFromName(m[1].toUpperCase());
    return env.ROOM.get(id).fetch(req);
  },
};

const send = (ws, o) => { try { ws.send(JSON.stringify(o)); } catch (e) {} };

export class Room {
  constructor(state) { this.state = state; }

  seatOf(ws) {
    const t = this.state.getTags(ws).find(x => /^s[1-3]$/.test(x));
    return t ? +t[1] : 0;
  }

  async fetch(req) {
    if (req.headers.get('Upgrade') !== 'websocket')
      return new Response('Expected WebSocket', { status: 426 });
    const role = new URL(req.url).searchParams.get('r');
    const host = this.state.getWebSockets('host')[0];
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    if (role === 'h') {
      if (host) {
        this.state.acceptWebSocket(server, ['x']);
        send(server, { sys: 'taken' });
        server.close(1000, 'taken');
      } else {
        this.state.acceptWebSocket(server, ['host']);
        send(server, { sys: 'hello' });
      }
    } else if (!host) {
      this.state.acceptWebSocket(server, ['x']);
      send(server, { sys: 'nohost' });
      server.close(1000, 'nohost');
    } else {
      const used = new Set(this.state.getWebSockets('g').map(w => this.seatOf(w)));
      let seat = 0;
      for (let i = 1; i <= 3; i++) if (!used.has(i)) { seat = i; break; }
      if (!seat) {
        this.state.acceptWebSocket(server, ['x']);
        send(server, { sys: 'full' });
        server.close(1000, 'full');
      } else {
        this.state.acceptWebSocket(server, ['g', 's' + seat]);
        send(host, { sys: 'join', seat });
      }
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    if (typeof msg !== 'string' || msg.length > 8192) return;
    let o;
    try { o = JSON.parse(msg); } catch (e) { return; }
    if (!o || typeof o !== 'object') return;
    const tags = this.state.getTags(ws);

    if (tags.includes('host')) {
      // хозяин -> гостям: {to:'*', ex:seat, d:...} или {to:seat, d:...}
      const pay = JSON.stringify({ d: o.d });
      if (o.to === '*') {
        for (const g of this.state.getWebSockets('g')) {
          if (o.ex && this.seatOf(g) === +o.ex) continue;
          try { g.send(pay); } catch (e) {}
        }
      } else {
        const g = this.state.getWebSockets('s' + (+o.to))[0];
        if (g) try { g.send(pay); } catch (e) {}
      }
    } else if (tags.includes('g')) {
      // гость -> хозяину
      const host = this.state.getWebSockets('host')[0];
      if (host) send(host, { from: this.seatOf(ws), d: o });
    }
  }

  async webSocketClose(ws) {
    const tags = this.state.getTags(ws);
    if (tags.includes('host')) {
      for (const g of this.state.getWebSockets('g')) {
        send(g, { sys: 'hostleft' });
        try { g.close(1000, 'host left'); } catch (e) {}
      }
    } else if (tags.includes('g')) {
      const host = this.state.getWebSockets('host')[0];
      if (host) send(host, { sys: 'leave', seat: this.seatOf(ws) });
    }
    try { ws.close(); } catch (e) {}
  }

  async webSocketError(ws) { await this.webSocketClose(ws); }
}
