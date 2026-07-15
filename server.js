const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const rooms = new Map(); // roomId → room object

const TEAM_COLORS = ['#3B82F6','#EF4444','#A855F7','#19C8B6','#F59E0B','#84CC16','#EC4899','#F97316'];
const POS_ORDER   = { TOP:0, JG:1, MID:2, ADC:3, SUP:4 };

function genId(n = 6) { return crypto.randomBytes(n).toString('base64url').slice(0, n).toUpperCase(); }
function genToken()   { return crypto.randomBytes(20).toString('hex'); }
function nowTime()    { return new Date().toLocaleTimeString('ko-KR',{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'}); }
function fmt(n)       { return (n||0).toLocaleString('ko-KR'); }

/* ─── API ──────────────────────────────────────────────────── */
app.post('/api/create-room', (req, res) => {
  const roomId    = genId(6);
  const hostToken = genToken();
  rooms.set(roomId, createRoom(roomId, hostToken));
  setTimeout(() => rooms.delete(roomId), 24 * 60 * 60 * 1000);
  res.json({ ok: true, roomId, hostToken });
});

app.get('/api/import-inhouse', async (req, res) => {
  const base = String(req.query.url || 'http://localhost:3000').replace(/\/+$/, '');
  try {
    const r = await fetch(`${base}/api/inhouse-db`, { signal: AbortSignal.timeout(5000) });
    if (!r.ok) return res.status(502).json({ ok: false, error: `내전사이트 응답 오류 (${r.status})` });
    const db = await r.json();
    const viewers = Array.isArray(db.viewers) ? db.viewers : [];

    // 채팅 투표 현황 (없거나 실패해도 무시 — 구버전 내전사이트 호환)
    let vote = null;
    try {
      const vr = await fetch(`${base}/api/vote-state`, { signal: AbortSignal.timeout(5000) });
      if (vr.ok) {
        const vd = await vr.json();
        if (vd.ok) vote = vd.vote;
      }
    } catch {}
    // !투표1 (첫 번째 항목)에 투표한 사람만 대상으로 함
    const voteItem = vote && Array.isArray(vote.items) ? vote.items[0] : null;
    const votedNames = new Set();
    if (voteItem) for (const n of (voteItem.votes || [])) votedNames.add(String(n).trim());

    const TIER_ELO = {
      IR4:600,IR3:613,IR2:626,IR1:639, BR4:660,BR3:673,BR2:686,BR1:699,
      SI4:720,SI3:737,SI2:754,SI1:771, GO4:800,GO3:817,GO2:834,GO1:851,
      PL4:875,PL3:892,PL2:909,PL1:926, EM4:960,EM3:982,EM2:1004,EM1:1026,
      DI4:1080,DI3:1119,DI2:1158,DI1:1197, GM:2100,CH:2300,
    };
    const VALID_POS = new Set(['TOP','JG','MID','ADC','SUP']);
    const POS_KO_MAP = { 탑:'TOP', 정글:'JG', 미드:'MID', 원딜:'ADC', 서포터:'SUP' };
    const toPos = raw => POS_KO_MAP[raw] || (VALID_POS.has(raw) ? raw : '');
    const players = viewers.filter(v => v.name).map(v => {
      const positions = Array.isArray(v.positions) ? v.positions : [];
      const rawPos = positions[0] || '';
      const pos = toPos(rawPos) || 'MID';
      const rawSub = positions[1] || '';
      const subPos = toPos(rawSub) || (rawSub === '무관' ? '무관' : '');
      const tierKey = String(v.tier || '');
      let elo = TIER_ELO[tierKey] || 0;
      if (!elo && tierKey.startsWith('MS')) elo = 1500 + (parseInt(tierKey.slice(2)) || 0);
      const voted = votedNames.has(String(v.chzzk || '').trim()) || votedNames.has(String(v.name || '').trim());
      return {
        nick: v.name, chzzk: v.chzzk || '', pos, subPos,
        tier: tierKey || '언랭', elo,
        mosts: Array.isArray(v.mosts) ? v.mosts.slice(0, 3) : [],
        voted, discordId: String(v.discordId || '').replace(/\D/g, ''),
        mic: v.mic || '불가',
      };
    });
    res.json({
      ok: true, players, total: viewers.length,
      vote: voteItem ? { active: !!vote.active, title: vote.title || '', itemLabel: voteItem.label || '', voterCount: votedNames.size } : null,
    });
  } catch (err) {
    res.status(502).json({ ok: false, error: '연결 실패: ' + err.message });
  }
});

app.get('/api/room/:id', (req, res) => {
  const room = rooms.get(req.params.id.toUpperCase());
  if (!room) return res.json({ ok: false, error: '방을 찾을 수 없습니다' });
  res.json({ ok: true, phase: room.phase, teamCount: room.teams.length });
});

/* ─── Room factory ─────────────────────────────────────────── */
function createRoom(roomId, hostToken) {
  const defaultTeams = ['1팀','2팀','3팀','4팀'].map((name, i) => ({
    id: 't' + (i + 1),
    name, color: TEAM_COLORS[i],
    points: 1000, roster: [], captainName: '',
  }));
  return {
    roomId, hostToken,
    phase: 'setup',
    config: { room: '경매내전', points: 1000, step: 50, timer: 30, addTime: 5, teamSize: 5 },
    step: 50,
    teams: defaultTeams,
    pool: [],
    currentId: null,
    bid: { amount: 0, teamId: null, history: [] },
    timer: { remaining: 30, base: 30, ref: null },
    log: [], chat: [],
    clients: new Map(), // ws → { role, teamId, name, id }
  };
}

/* ─── WebSocket ────────────────────────────────────────────── */
wss.on('connection', ws => {
  let room = null;
  let me   = null;

  ws.on('message', raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }

    /* ── hello: identify room ── */
    if (msg.type === 'hello') {
      const r = rooms.get((msg.roomId || '').toUpperCase());
      if (!r) { send(ws, { type:'error', msg:'방을 찾을 수 없습니다. URL을 확인하세요.' }); return; }
      room = r;

      // 라이브 데모 방: 역할 선택 없이 바로 관전자로 입장
      if (room.demo) {
        me = { role:'viewer', pendingHost:false, teamId:null, name:'관전자', id: genId(8) };
        room.clients.set(ws, me);
        send(ws, { type:'state', state: toState(room) });
        send(ws, { type:'welcome', you:{ role:'viewer', teamId:null, id:me.id } });
        return;
      }

      const isHost = msg.hostToken && msg.hostToken === room.hostToken;
      me = { role: 'pending', pendingHost: isHost, teamId: null, name: '', id: genId(8) };
      room.clients.set(ws, me);
      // 진행자도 오버레이 선택 화면을 거치도록 — welcome은 join 후 전송
      send(ws, { type:'init', state: toState(room), canHost: isHost });
      return;
    }

    if (!room || !me) return;

    /* ── join: pick role/team ── */
    if (msg.type === 'join') {
      if (me.role !== 'pending') return;
      const name = String(msg.name || '').trim().slice(0, 20);
      if (!name) { send(ws, { type:'error', msg:'닉네임을 입력하세요' }); return; }

      if (msg.role === 'host') {
        if (!me.pendingHost) { send(ws, { type:'error', msg:'진행자 권한이 없습니다' }); return; }
        me.role = 'host'; me.name = name || '진행자';
      } else if (msg.role === 'captain') {
        me.role = 'captain'; me.name = name;
      } else {
        send(ws, { type:'error', msg:'올바른 역할을 선택해주세요' }); return;
      }
      send(ws, { type:'welcome', you:{ role:me.role, teamId:me.teamId, id:me.id } });
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }

    /* ── host: config ── */
    if (msg.type === 'setConfig') {
      if (me.role !== 'host') return;
      Object.assign(room.config, msg.config);
      room.step = room.config.step;
      // 팀별로 따로 설정한 포인트는 유지하고, 나머지 팀에만 새 기본 포인트 적용
      room.teams.forEach(t => { if (!t.customPoints) t.points = room.config.points; });
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }

    /* ── host: team management ── */
    if (msg.type === 'addTeam') {
      if (me.role !== 'host' || room.teams.length >= 8) return;
      room.teams.push({
        id: 't'+Date.now(), name: String(msg.name||'').trim().slice(0,20)||'팀'+(room.teams.length+1),
        color: TEAM_COLORS[room.teams.length % TEAM_COLORS.length],
        points: room.config.points, roster: [], claimed: false, captainName: '',
      });
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }
    if (msg.type === 'delTeam') {
      if (me.role !== 'host') return;
      room.teams = room.teams.filter(t => t.id !== msg.teamId);
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }
    if (msg.type === 'editTeam') {
      if (me.role !== 'host') return;
      const t = room.teams.find(t => t.id === msg.teamId);
      if (t && msg.name) t.name = String(msg.name).trim().slice(0, 20);
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }

    /* ── host: per-team starting points override ── */
    if (msg.type === 'setTeamPoints') {
      if (me.role !== 'host') return;
      const t = room.teams.find(t => t.id === msg.teamId);
      if (!t) return;
      t.points = Math.max(0, Math.floor(+msg.points) || 0);
      t.customPoints = true;
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }

    /* ── host: assign captain to team ── */
    if (msg.type === 'assignCaptain') {
      if (me.role !== 'host') return;
      const team = room.teams.find(t => t.id === msg.teamId);
      if (!team) return;
      let targetMe = null, targetWs = null;
      for (const [cws, client] of room.clients) {
        if (client.id === msg.captainId) { targetMe = client; targetWs = cws; break; }
      }
      if (!targetMe || targetMe.role !== 'captain') return;
      // unassign from previous team
      if (targetMe.teamId) {
        const old = room.teams.find(t => t.id === targetMe.teamId);
        if (old) old.captainName = '';
      }
      // bump any existing captain from target team
      for (const [, client] of room.clients) {
        if (client !== targetMe && client.role === 'captain' && client.teamId === team.id) {
          client.teamId = null;
        }
      }
      targetMe.teamId = team.id;
      team.captainName = targetMe.name;
      // pool에서 팀장의 discordId 조회 (nick 또는 chzzk로 매칭)
      const capInPool = room.pool.find(p => p.nick === targetMe.name || p.chzzk === targetMe.name);
      const capDiscordId = capInPool?.discordId || targetMe.discordId || '';
      team.captainDiscordId = capDiscordId;
      // 다른 팀에 동일 이름/discordId로 남아 있는 임포트 팀장 제거
      for (const t of room.teams) {
        if (t.id === team.id) continue;
        if (t.captainName === targetMe.name || (capDiscordId && t.captainDiscordId === capDiscordId)) {
          t.captainName = '';
          t.captainDiscordId = '';
        }
      }
      send(targetWs, { type: 'assigned', teamId: team.id });
      broadcast(room, { type: 'state', state: toState(room) });
      return;
    }

    /* ── host: bulk import from inhouse site ── */
    if (msg.type === 'importPlayers') {
      if (me.role !== 'host') return;
      let added = 0;
      for (const p of (msg.players || [])) {
        const nick = String(p.nick || '').trim().slice(0, 20);
        if (!nick || room.pool.some(x => x.nick === nick)) continue;
        room.pool.push({
          id: 'p' + Date.now() + '_' + Math.floor(Math.random() * 99999),
          nick, chzzk: String(p.chzzk || '').trim().slice(0, 80),
          pos: p.pos || 'MID', subPos: String(p.subPos || ''),
          tier: String(p.tier || '').slice(0, 12) || '언랭',
          elo: +p.elo || 0,
          mosts: Array.isArray(p.mosts) ? p.mosts.slice(0, 3).map(m => String(m).slice(0, 30)) : [],
          discordId: String(p.discordId || '').replace(/\D/g, ''),
          mic: String(p.mic || '').slice(0, 12),
          status: 'wait', soldTo: null, price: 0, passCount: 0,
        });
        added++;
      }
      // 웹소켓으로 입장한 팀장의 discordId가 없으면 pool에서 nick/chzzk 매칭으로 채움
      for (const team of room.teams) {
        if (team.captainName && !team.captainDiscordId) {
          const match = room.pool.find(p =>
            p.nick === team.captainName || p.chzzk === team.captainName
          );
          if (match?.discordId) team.captainDiscordId = match.discordId;
        }
      }
      // 이미 팀장으로 등록된 discordId/이름 수집 (중복 방지)
      const usedDiscordIds = new Set(room.teams.map(t => t.captainDiscordId).filter(Boolean));
      const usedNames = new Set(room.teams.map(t => t.captainName).filter(Boolean));
      const captains = (msg.captains || []).filter(c => String(c.name || '').trim());
      let skipped = 0;
      for (let i = 0; i < captains.length && i < room.teams.length; i++) {
        const capName = String(captains[i].name || '').trim().slice(0, 20);
        const discordId = String(captains[i].discordId || '').replace(/\D/g, '');
        if ((discordId && usedDiscordIds.has(discordId)) || usedNames.has(capName)) {
          skipped++;
          continue;
        }
        room.teams[i].captainName = capName;
        room.teams[i].captainDiscordId = discordId;
        if (discordId) usedDiscordIds.add(discordId);
        usedNames.add(capName);
      }
      if (skipped) send(ws, { type: 'toast', msg: `중복 팀장 ${skipped}명 자동 제외 (이미 다른 팀에 존재)` });
      broadcast(room, { type: 'state', state: toState(room) });
      return;
    }

    /* ── host: player management ── */
    if (msg.type === 'addPlayer') {
      if (me.role !== 'host') return;
      const nick = String(msg.nick||'').trim().slice(0,20);
      if (!nick) return;
      room.pool.push({
        id: 'p'+Date.now()+Math.floor(Math.random()*9999),
        nick, chzzk: '', pos: msg.pos||'MID', subPos: '',
        tier: String(msg.tier||'').trim().slice(0,12)||'언랭',
        elo: +msg.elo||0, mosts: [], status:'wait', soldTo:null, price:0, passCount:0,
      });
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }
    if (msg.type === 'delPlayer') {
      if (me.role !== 'host') return;
      room.pool = room.pool.filter(p => p.id !== msg.playerId);
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }

    /* ── host: start auction ── */
    if (msg.type === 'startAuction') {
      if (me.role !== 'host' || room.phase !== 'setup') return;
      if (room.teams.length < 2) { send(ws,{type:'toast',msg:'팀이 2개 이상이어야 합니다'}); return; }
      if (!room.pool.length)     { send(ws,{type:'toast',msg:'매물이 1명 이상이어야 합니다'}); return; }
      room.phase = 'auction';
      // reset team points to config (팀별로 따로 설정한 포인트는 유지)
      room.teams.forEach(t => { if (t.roster.length === 0 && !t.customPoints) t.points = room.config.points; });
      pushLog(room, { type:'sys', text:'<b>경매 시작!</b> 매물이 랜덤으로 등장합니다.' });
      broadcast(room, { type:'state', state:toState(room) });
      const next = pickFromWait(room);
      if (next) setTimeout(() => startLot(room, next.id), 800);
      return;
    }

    /* ── host: auction controls ── */
    if (msg.type === 'startLot') {
      if (me.role !== 'host' || room.phase !== 'auction') return;
      startLot(room, msg.playerId);
      return;
    }
    if (msg.type === 'setStep') {
      if (me.role !== 'host') return;
      room.step = +msg.step || room.config.step;
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }
    if (msg.type === 'hostBid') {
      if (me.role !== 'host' || room.phase !== 'auction') return;
      doBid(room, msg.teamId, ws);
      return;
    }
    if (msg.type === 'sell') {
      if (me.role !== 'host' || room.phase !== 'auction') return;
      sellLot(room);
      return;
    }
    if (msg.type === 'pass') {
      if (me.role !== 'host' || room.phase !== 'auction') return;
      passLot(room);
      return;
    }
    if (msg.type === 'undo') {
      if (me.role !== 'host' || room.phase !== 'auction') return;
      if (!room.bid.history.length) { send(ws,{type:'toast',msg:'취소할 입찰이 없습니다'}); return; }
      const prev = room.bid.history.pop();
      room.bid = { amount: prev.amount, teamId: prev.teamId, history: room.bid.history };
      broadcast(room, { type:'state', state:toState(room) });
      return;
    }

    /* ── host: result screen - 디스코드 음성 이동 + 역할 부여 ── */
    if (msg.type === 'moveDiscordTeams') {
      if (me.role !== 'host' || room.phase !== 'ended') return;
      const base = String(msg.inhouseUrl || '').trim().replace(/\/+$/, '') || 'http://localhost:3000';
      const teams = room.teams.map(t => ({
        name: t.name,
        discordIds: [t.captainDiscordId, ...t.roster.map(p => p.discordId)].filter(Boolean),
      }));
      if (!teams.some(t => t.discordIds.length)) {
        send(ws, { type:'toast', msg:'디스코드 연동된 팀원이 없습니다' });
        return;
      }
      (async () => {
        try {
          send(ws, { type:'toast', msg:'🔊 디스코드 이동 요청 중…' });
          const r = await fetch(`${base}/api/auction-move-voice-teams`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'x-viewer-secret': process.env.VIEWER_SERVER_SECRET || 'davido-admin',
            },
            body: JSON.stringify({ teams }),
            signal: AbortSignal.timeout(40000),
          });
          const data = await r.json();
          if (data.ok) {
            const movedDetail = data.moved ? Object.entries(data.moved).map(([n,c]) => `${n}:${c}명`).join(' ') : '';
            const movedTotal = Object.values(data.moved || {}).reduce((a,b) => a + (+b || 0), 0);
            const errNote = data.errors?.length ? ` (일부 실패: ${data.errors.join(', ')})` : '';
            broadcast(room, { type:'toast', msg:`🔊 이동 완료 ${movedTotal}명 — ${movedDetail}${errNote}` });
          } else {
            send(ws, { type:'toast', msg:'❌ 디스코드 이동 실패: ' + (data.error || '알 수 없는 오류') });
          }
        } catch (err) {
          const hint = err.name === 'TimeoutError' ? '(타임아웃 — 봇 응답 없음)' : err.message;
          send(ws, { type:'toast', msg:'❌ 디스코드 이동 실패: ' + hint });
        }
      })();
      return;
    }

    /* ── captain: bid ── */
    if (msg.type === 'bid') {
      if (me.role !== 'captain' || room.phase !== 'auction') return;
      doBid(room, me.teamId, ws);
      return;
    }

    /* ── chat ── */
    if (msg.type === 'chat') {
      if (!me.name) return;
      const text = String(msg.text||'').trim().slice(0,200);
      if (!text) return;
      const t = room.teams.find(x => x.id === me.teamId);
      room.chat.push({ user: me.name, text, role: me.role, color: t?.color || null });
      if (room.chat.length > 150) room.chat.shift();
      broadcast(room, { type:'chat_update', chat: room.chat });
      return;
    }
  });

  ws.on('close', () => {
    if (!room || !me) return;
    if (me.role === 'captain' && me.teamId) {
      const team = room.teams.find(t => t.id === me.teamId);
      if (team) team.captainName = '';
    }
    room.clients.delete(ws);
    broadcast(room, { type:'state', state:toState(room) });
  });
});

/* ─── Game logic ───────────────────────────────────────────── */
function doBid(room, teamId, senderWs) {
  const team = room.teams.find(t => t.id === teamId);
  if (!team) return;
  if (!room.currentId)                            { send(senderWs,{type:'toast',msg:'진행 중인 매물이 없습니다'}); return; }
  if (team.roster.length >= room.config.teamSize - 1) { send(senderWs,{type:'toast',msg:`${team.name}은(는) 팀이 완성됐습니다`}); return; }
  if (room.bid.teamId === teamId)                 { send(senderWs,{type:'toast',msg:'이미 최고 입찰 팀입니다'}); return; }
  const next = (room.bid.teamId ? room.bid.amount : 0) + room.step;
  if (team.points < next) { send(senderWs,{type:'toast',msg:`${team.name} 포인트 부족 (${fmt(team.points)}P 보유)`}); return; }

  room.bid.history.push({ amount: room.bid.amount, teamId: room.bid.teamId });
  room.bid.amount = next; room.bid.teamId = teamId;
  room.timer.remaining = Math.min(room.config.timer, room.timer.remaining + room.config.addTime);
  pushLog(room, { type:'bid', teamId, amount: next });
  broadcast(room, { type:'state', state:toState(room) });
}

function startLot(room, id) {
  stopTimer(room);
  room.currentId = id;
  room.bid = { amount:0, teamId:null, history:[] };
  room.timer.remaining = room.config.timer;
  room.timer.base = room.config.timer;
  const lot = room.pool.find(p => p.id === id);
  if (lot) pushLog(room, { type:'sys', text:`<b>${lot.nick}</b> (${lot.pos}·${lot.tier}) 경매 시작` });
  broadcast(room, { type:'state', state:toState(room) });
  startTimer(room);
}

function sellLot(room) {
  const lot = room.pool.find(p => p.id === room.currentId);
  if (!lot || !room.bid.teamId) return;
  stopTimer(room);
  const team = room.teams.find(t => t.id === room.bid.teamId);
  team.points -= room.bid.amount;
  team.roster.push({ nick:lot.nick, pos:lot.pos, tier:lot.tier, elo:lot.elo, price:room.bid.amount, discordId:lot.discordId || '' });
  team.roster.sort((a,b) => (POS_ORDER[a.pos]??9) - (POS_ORDER[b.pos]??9));
  lot.status = 'sold'; lot.soldTo = team.id; lot.price = room.bid.amount;
  pushLog(room, { type:'sold', teamId:team.id, nick:lot.nick, amount:room.bid.amount });
  broadcast(room, { type:'toast', msg:`🔨 ${lot.nick} → ${team.name} 낙찰 (${fmt(room.bid.amount)}P)` });
  advanceLot(room);
}

function passLot(room) {
  const lot = room.pool.find(p => p.id === room.currentId);
  if (!lot) return;
  stopTimer(room);
  lot.status = 'wait';
  lot.passCount = (lot.passCount || 0) + 1;
  // 유찰된 매물은 대기열 맨 뒤로 이동시켜 다른 매물이 먼저 나오게 함
  room.pool = room.pool.filter(p => p.id !== lot.id);
  room.pool.push(lot);
  pushLog(room, { type:'pass', nick:lot.nick });
  broadcast(room, { type:'toast', msg:`${lot.nick} 유찰 (대기열 맨 뒤로 이동)` });
  advanceLot(room);
}

function pickFromWait(room) {
  const wait = room.pool.filter(p => p.status === 'wait');
  if (!wait.length) return null;
  const minPass = Math.min(...wait.map(p => p.passCount || 0));
  const candidates = wait.filter(p => (p.passCount || 0) === minPass);
  return candidates[Math.floor(Math.random() * candidates.length)];
}

function advanceLot(room) {
  room.currentId = null;
  room.bid = { amount:0, teamId:null, history:[] };
  const next = pickFromWait(room);
  if (next) {
    broadcast(room, { type:'state', state:toState(room) });
    setTimeout(() => startLot(room, next.id), 1500);
  } else {
    room.phase = 'ended';
    pushLog(room, { type:'sys', text:'<b>경매 종료!</b> 최종 팀 구성을 확인하세요.' });
    broadcast(room, { type:'state', state:toState(room) });
    broadcast(room, { type:'toast', msg:'경매 종료! 결과 탭을 확인하세요 🏆' });
    if (room.demo) setTimeout(() => resetDemo(room), 15000);
  }
}

/* ─── Timer ────────────────────────────────────────────────── */
function startTimer(room) {
  stopTimer(room);
  const TICK = 250;
  room.timer.ref = setInterval(() => {
    room.timer.remaining -= TICK / 1000;
    if (room.timer.remaining <= 0) {
      room.timer.remaining = 0;
      stopTimer(room);
      broadcast(room, { type:'tick', remaining:0 });
      if (room.bid.teamId) sellLot(room); else passLot(room);
      return;
    }
    broadcast(room, { type:'tick', remaining: room.timer.remaining });
  }, TICK);
}
function stopTimer(room) {
  if (room.timer.ref) { clearInterval(room.timer.ref); room.timer.ref = null; }
}

/* ─── Helpers ──────────────────────────────────────────────── */
function pushLog(room, entry) {
  entry.time = nowTime();
  room.log.push(entry);
  if (room.log.length > 300) room.log.shift();
}

function toState(room) {
  const online = new Set([...room.clients.values()].filter(c=>c.teamId).map(c=>c.teamId));
  const pendingCaptains = [...room.clients.values()]
    .filter(c => c.role === 'captain' && !c.teamId)
    .map(c => ({ id: c.id, name: c.name }));
  return {
    phase: room.phase, config: room.config, step: room.step,
    teams: room.teams.map(t => ({ ...t, online: online.has(t.id) })),
    pool: room.pool, currentId: room.currentId,
    bid: { amount: room.bid.amount, teamId: room.bid.teamId },
    timer: { remaining: room.timer.remaining, base: room.timer.base || room.config.timer },
    log: room.log, chat: room.chat,
    pendingCaptains,
  };
}

function send(ws, obj)          { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function broadcast(room, obj)   { const d = JSON.stringify(obj); for (const [ws] of room.clients) send(ws, JSON.parse(d)); }

/* ─── Live demo room ───────────────────────────────────────── */
const DEMO_CHAMPS = ['Ahri','Yasuo','Jinx','Lux','Garen','Darius','Leona','Riven','Zed','Vayne','Ezreal','Thresh','Annie','Ashe','Lulu','Nasus','Sona','Talon','Vi','Xayah'];
const DEMO_TIERS  = [['Challenger',2300],['Grandmaster',2100],['Diamond 1',1197],['Diamond 3',1119],['Emerald 2',1004],['Platinum 1',926],['Platinum 3',892],['Gold 2',834],['Gold 4',800],['Silver 2',754],['Silver 4',720],['Bronze 2',686]];
const DEMO_NAMES  = [
  ['몰루겐','Molgen'],['피넛불주먹','PeanutPunch'],['고요속외침','SilentScream'],['우유한잔','MilkCarton'],
  ['정글동선','JungleRoute'],['칼퇴를위해','EarlyLeave'],['빛돌이','LightBoy'],['새벽감성','DawnVibe'],
  ['무지개반사','RainbowFlex'],['한타장인','TeamfightPro'],['라인전귀신','LaneGhost'],['서폿하는남자','SupportGuy'],
  ['원딜의품격','ADCKing'],['미드라이너','MidLaner'],['정글차이','JglDiff'],['탑신병자','TopCrazy'],
  ['칼바람단골','ARAMRegular'],['솔랭귀신','SoloQGhost'],['데미지딜러','DmgDealer'],['막타장인','LastHitMaster'],
];

function createDemoRoom() {
  const room = createRoom('DEMO01', '');
  room.demo = true;
  room.config = { room: 'BIDRIFT 라이브 데모', points: 1000, step: 50, timer: 12, addTime: 2, teamSize: 5 };
  room.step = room.config.step;

  const POS5 = ['TOP','JG','MID','ADC','SUP'];
  room.pool = DEMO_NAMES.map(([chzzk, nick], i) => {
    const pos = POS5[Math.floor(i / 4)];
    const subPos = POS5[(Math.floor(i / 4) + 1) % 5];
    const [tier, elo] = DEMO_TIERS[i % DEMO_TIERS.length];
    const mic = ['가능','가능','부분가능','불가'][i % 4];
    return {
      id: 'demo' + i,
      nick, chzzk, pos, subPos, tier, elo,
      mosts: [DEMO_CHAMPS[i % 20], DEMO_CHAMPS[(i + 3) % 20], DEMO_CHAMPS[(i + 7) % 20]],
      mic, status: 'wait', soldTo: null, price: 0, passCount: 0,
    };
  });

  room.teams.forEach((t, i) => { t.captainName = `데모팀장${i + 1}`; t.points = room.config.points; });
  room.phase = 'auction';
  pushLog(room, { type:'sys', text:'<b>라이브 데모 경매가 진행 중입니다.</b>' });
  return room;
}

function pickNextLot(room, delay) {
  const next = pickFromWait(room);
  if (next) setTimeout(() => startLot(room, next.id), delay);
}

function botTick(room) {
  if (room.phase !== 'auction' || !room.currentId) return;
  const next = (room.bid.teamId ? room.bid.amount : 0) + room.step;
  const candidates = room.teams.filter(t =>
    t.id !== room.bid.teamId &&
    t.roster.length < room.config.teamSize - 1 &&
    t.points >= next
  );
  if (!candidates.length || Math.random() < 0.45) return;
  const team = candidates[Math.floor(Math.random() * candidates.length)];
  doBid(room, team.id, null);
}

function resetDemo(room) {
  room.pool.forEach(p => { p.status = 'wait'; p.soldTo = null; p.price = 0; p.passCount = 0; });
  room.teams.forEach(t => { t.roster = []; t.points = room.config.points; });
  room.currentId = null;
  room.bid = { amount:0, teamId:null, history:[] };
  room.phase = 'auction';
  pushLog(room, { type:'sys', text:'<b>라이브 데모 경매를 다시 시작합니다.</b>' });
  broadcast(room, { type:'state', state: toState(room) });
  pickNextLot(room, 1500);
}

function startDemoLoop(room) {
  pickNextLot(room, 800);
  room.botRef = setInterval(() => botTick(room), 1100);
}

/* ─── Start ────────────────────────────────────────────────── */
const demoRoom = createDemoRoom();
rooms.set(demoRoom.roomId, demoRoom);
startDemoLoop(demoRoom);

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => console.log(`[AUCTION] http://localhost:${PORT}`));
