// Ретранслятор комнат для «Тихой поездки» (Cloudflare Worker + Durable Object)
// Хозяин и гости соединяются по WebSocket, сервер только пересылает сообщения.

/* ===== ИИ-сосед: маршрут /ai ===== */
const CORS={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST, OPTIONS','Access-Control-Allow-Headers':'Content-Type'};
const SYSTEM=`Ты — стикмен, пассажир на переднем сиденье в уютной спокойной игре «Тихая поездка». Ты дружелюбный, чуть ироничный болтун. Отвечай по-русски, 1–2 коротких предложения, без списков и markdown. На заднем сиденье едет кот. Реагируй на происходящее в игре (скорость, ночь, дождь, город), но не пересказывай данные, как робот. Если игрок сбил пешехода — отреагируй в образе (шокированно, но в рамках игры), без подробностей насилия.`;

const MODELS=['@cf/google/gemma-3-12b-it','@cf/meta/llama-3.1-8b-instruct'];
async function handleAI(request, env){
  const J={...CORS,'content-type':'application/json'};
  if(request.method==='OPTIONS') return new Response(null,{headers:CORS});
  if(request.method!=='POST') return new Response('POST only',{status:405,headers:CORS});
  if(!env.AI) return new Response(JSON.stringify({reply:'',error:'no AI binding'}),{status:500,headers:J});
  let b; try{ b=await request.json() }catch(e){ return new Response('bad json',{status:400,headers:CORS}) }
  const msgs=(Array.isArray(b.messages)?b.messages:[]).slice(-10)
    .map(m=>({role:m.role==='assistant'?'assistant':'user',content:String(m.content||'').slice(0,300)}))
    .filter(m=>m.content);
  while(msgs.length&&msgs[0].role!=='user') msgs.shift();
  if(!msgs.length) return new Response('no messages',{status:400,headers:CORS});
  const messages=[{role:'system',content:SYSTEM+'\nСейчас в игре: '+JSON.stringify(b.ctx||{})},...msgs];
  let reply='';
  for(const model of MODELS){
    try{
      const r=await env.AI.run(model,{messages,max_tokens:150});
      reply=String(r.response||(r.choices&&r.choices[0]&&r.choices[0].message&&r.choices[0].message.content)||'').trim();
      if(reply) break;
    }catch(e){}
  }
  return new Response(JSON.stringify({reply}),{headers:J});
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/ai') return handleAI(req, env);
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
