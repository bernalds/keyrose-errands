/* Errands: optional accounts and sync between phones (Supabase).
   Runs after the main script and reuses its save queue: write() -> queue -> flush() -> db.
   With no account, db stays null and the app works on this phone only, as before. */
const SB_URL = "https://gvqbsbydijgxocjddjaz.supabase.co";
const SB_KEY = "sb_publishable_cPWIZmVf44Zuv2J8nCkCkQ_J5WPtN5-";          // public anon key; row-level security protects the data
const PFX = "gp.";                    // localStorage prefix (matches index.html)
const AUTH_KEY = PFX + "auth";

const acct = { user:null, hid:null, name:null, code:null, members:1, status:"off", sent:null, form:{} };
let sb = null, chan = null, remote = { items:{}, trips:{}, state:null, loaded:false };

function sbReady(){
  if(sb) return sb;
  if(!window.supabase || SB_URL.startsWith("__")) return null;
  sb = supabase.createClient(SB_URL, SB_KEY, { auth:{ storageKey:AUTH_KEY, flowType:"implicit", persistSession:true, autoRefreshToken:true, detectSessionInUrl:true } });
  return sb;
}
const now = () => new Date().toISOString();
const clone = o => JSON.parse(JSON.stringify(o));

/* ---------- Database adapter with the same shape the app already uses ---------- */
function makeDb(hid){
  const run = async q => { const { error } = await q; if(error) throw { code: error.code==="22P02"?"invalid_argument":(error.code||"net"), message:error.message }; };
  const split = p => p==="app/state" ? ["settings",null] : [p.slice(0,p.indexOf("/")), p.slice(p.indexOf("/")+1)];
  return {
    doc(path){
      const [t,id] = split(path);
      return {
        set: body => run(t==="settings"
          ? sb.from("settings").upsert({ household_id:hid, data:body, updated_at:now() })
          : sb.from(t).upsert({ household_id:hid, id, data:body, deleted:false, updated_at:now() })),
        delete: () => run(sb.from(t).upsert({ household_id:hid, id, data:null, deleted:true, updated_at:now() })),
      };
    }
  };
}

/* ---------- Reading: full pull, then live changes ---------- */
async function fetchAll(table){
  const out = {}; const N = 1000;
  for(let from=0;;from+=N){
    const { data, error } = await sb.from(table).select("id,data").eq("household_id",acct.hid).eq("deleted",false).order("id").range(from,from+N-1);
    if(error) throw error;
    data.forEach(r => { if(r.data) out[r.id] = r.data; });
    if(data.length < N) return out;
  }
}
const snapColl = m => ({ docs: Object.entries(m).map(([id,d]) => ({ id, data:()=>d })) });
const snapState = () => ({ exists: !!remote.state, data:()=>remote.state, metadata:{ fromCache:false } });

async function pull(){
  if(!acct.hid || !db) return;
  try{
    const [items, trips, st] = await Promise.all([ fetchAll("items"), fetchAll("trips"),
      sb.from("settings").select("data").eq("household_id",acct.hid).maybeSingle() ]);
    if(st.error) throw st.error;
    remote.items = items; remote.trips = trips; remote.state = st.data ? st.data.data : null;
    if(!remote.loaded){ remote.loaded = true; if(!remote.state && !("app/state" in queue)) putState(); }
    applyRemote("state", snapState()); applyRemote("items", snapColl(remote.items)); applyRemote("trips", snapColl(remote.trips));
    flush();
  }catch(e){ setSync("off"); }
}
function onChange(kind, p){
  const r = p.new; if(!r) return;
  if(kind==="settings"){ remote.state = r.data; applyRemote("state", snapState()); return; }
  if(r.deleted || !r.data) delete remote[kind][r.id]; else remote[kind][r.id] = r.data;
  applyRemote(kind, snapColl(remote[kind]));
}
function subscribe(){
  if(chan) sb.removeChannel(chan);
  const f = "household_id=eq."+acct.hid;
  chan = sb.channel("hh-"+acct.hid);
  ["items","trips","settings"].forEach(t => chan.on("postgres_changes", { event:"*", schema:"public", table:t, filter:f }, p => onChange(t,p)));
  chan.subscribe(s => { if(s==="SUBSCRIBED") pull(); else if(s==="CHANNEL_ERROR"||s==="TIMED_OUT") setSync("off"); });
}
addEventListener("online", () => { if(db) pull(); });
document.addEventListener("visibilitychange", () => { if(document.visibilityState==="visible" && db) pull(); });

/* ---------- Signing in and out ---------- */
function queueAllLocal(withState){
  Object.values(S.items).forEach(it => { if(!("items/"+it.id in queue)) queue["items/"+it.id] = clone(it); });
  Object.values(S.trips).forEach(t => { if(!("trips/"+t.id in queue)) queue["trips/"+t.id] = clone(t); });
  if(withState) queue["app/state"] = clone(S.state); else delete queue["app/state"];
  saveLocal();
}
function firstLink(info){
  if(lsGet(PFX+"linked") === info.id) return;
  const ni = Object.keys(S.items).length, nt = Object.keys(S.trips).length;
  if(ni + nt > 0){
    const what = ni+" item"+(ni===1?"":"s")+" and "+nt+" trip"+(nt===1?"":"s");
    const ok = confirm(info.fresh
      ? "Save the list on this phone ("+what+") to your new account?\n\nCancel starts the account empty. A backup file of this phone's list is saved first."
      : "Your account already has a list. Add this phone's "+what+" to it?\n\nCancel shows the account's list instead. A backup file of this phone's list is saved first.");
    if(ok) queueAllLocal(info.fresh);
    else { exportData(); for(const k in queue) delete queue[k]; S.items = {}; S.trips = {}; saveLocal(); }
  } else if(!info.fresh){ for(const k in queue) delete queue[k]; saveLocal(); }
  lsSet(PFX+"linked", info.id);
}
function useHousehold(info){
  acct.hid = info.id; acct.name = info.name; acct.code = info.invite_code; acct.members = info.members||1;
  lsSet(PFX+"acct", { uid:acct.user.id, id:info.id, name:info.name, invite_code:info.invite_code, members:acct.members });
}
async function connect(user){
  if(acct.user && acct.user.id===user.id && db) return;
  acct.user = user; acct.status = "connecting"; setSync("busy");
  const cached = lsGet(PFX+"acct");
  if(cached && cached.uid===user.id && lsGet(PFX+"linked")===cached.id){ useHousehold(cached); start(); }   // works offline
  const { data, error } = await sb.rpc("ensure_household");
  if(error){ if(!db){ acct.status="retry"; setSync("off"); setTimeout(()=>{ acct.user=null; connect(user); },15000); } render(); return; }
  const moved = acct.hid && acct.hid!==data.id;
  firstLink(data); useHousehold(data);
  if(!db || moved) start(); else render();
}
function start(){
  db = makeDb(acct.hid); remote = { items:{}, trips:{}, state:null, loaded:false };
  acct.status = "on"; subscribe(); flush(); render();
}
function resetLocal(){
  if(chan){ sb.removeChannel(chan); chan = null; }
  db = null; acct.user = null; acct.hid = null; acct.status = "off";
  for(const k in queue) delete queue[k];
  S.state = defaultState(); S.items = {}; S.trips = {};
  localStorage.removeItem(PFX+"linked"); localStorage.removeItem(PFX+"acct");
  saveLocal(); setSync("local"); render();
}

async function sendLink(email){
  email = (email||"").trim(); if(!/^\S+@\S+\.\S+$/.test(email)){ toast("Enter your email address"); return; }
  setSync("busy");
  const { error } = await sb.auth.signInWithOtp({ email, options:{ emailRedirectTo: location.origin+location.pathname } });
  setSync(db?"ok":"local");
  if(error){ toast(/rate|seconds/i.test(error.message)?"Too many emails sent. Wait a few minutes and try again.":"Couldn't send the email. Check your connection."); return; }
  acct.sent = email; render(); toast("Check your email for the sign-in link");
}
async function verifyCode(code){
  code = (code||"").replace(/\D/g,""); if(code.length<6){ toast("Enter the code from the email"); return; }
  const { error } = await sb.auth.verifyOtp({ email:acct.sent, token:code, type:"email" });
  if(error) toast("That code didn't work. Codes expire after an hour.");
}
async function signOut(){
  const n = Object.keys(queue).length;
  if(!confirm((n ? n+" change"+(n===1?" hasn't":"s haven't")+" uploaded yet and will be lost.\n\n" : "")+"Sign out? This phone's copy of the list is cleared. Your list stays in your account.")) return;
  await sb.auth.signOut().catch(()=>{}); resetLocal(); toast("Signed out");
}
async function joinHousehold(code){
  code = (code||"").trim().toUpperCase(); if(code.length<6){ toast("Enter the invite code"); return; }
  if(!confirm("Join the household with code "+code+"?\n\nYour list and history will be added to theirs, and you'll share one list from now on.")) return;
  const { data, error } = await sb.rpc("join_household", { code });
  if(error){ toast(/No household/i.test(error.message)?"No household has that code":"Couldn't join. Check your connection."); return; }
  if(chan){ sb.removeChannel(chan); chan = null; }
  db = null; queueAllLocal(false); lsSet(PFX+"linked", data.id); useHousehold(data); start();
  acct.form.join = ""; toast("Joined. Your lists are combined.");
}
async function deleteAccount(){
  const shared = acct.members > 1;
  if(!confirm("Delete your account?\n\n"+(shared
      ? "You'll leave the shared household. The shared list stays for the others."
      : "Your list, history, stores and settings are deleted from the cloud.")+" This can't be undone.")) return;
  const { error } = await sb.rpc("delete_my_account");
  if(error){ toast("Couldn't delete the account. Check your connection."); return; }
  await sb.auth.signOut().catch(()=>{}); resetLocal(); toast("Account deleted");
}
async function copyCode(){
  try{ await navigator.clipboard.writeText(acct.code); toast("Invite code copied"); }catch(e){ toast("Invite code: "+acct.code); }
}

/* ---------- Account section on the Setup tab ---------- */
function accountSection(){
  const s = el('<section class="sect"><h2>Account and sync</h2><div class="card"></div><p class="hint"></p></section>');
  const card = s.querySelector(".card"), hint = s.querySelector(".hint");
  if(!sbReady()){ s.hidden = true; return s; }
  if(acct.user){
    card.appendChild(el('<div class="line"><span>Signed in as<br><span class="muted">'+esc(acct.user.email)+'</span></span><button class="mini" id="ac-out">Sign out</button></div>'));
    if(acct.code){
      card.appendChild(el('<div class="line"><span>Invite code'+(acct.members>1?' <span class="muted">('+acct.members+' people)</span>':'')+'<br><b style="letter-spacing:.12em">'+esc(acct.code)+'</b></span><button class="mini" id="ac-copy">Copy</button></div>'));
      const j = el('<div class="line"><input id="ac-join" placeholder="Code from someone else" autocapitalize="characters" autocomplete="off" style="flex:1;min-width:0"><button class="mini" id="ac-joinb">Join</button></div>');
      card.appendChild(j);
    } else card.appendChild(el('<div class="line"><span class="muted">'+(acct.status==="retry"?"Can't reach the server. Trying again…":"Connecting…")+'</span></div>'));
    card.appendChild(el('<div class="line"><span>Delete my account and data</span><button class="mini" id="ac-del" style="color:var(--danger)">Delete…</button></div>'));
    hint.textContent = "Changes sync to everyone in your household within a few seconds. To share, give someone your invite code; they sign in and tap Join.";
  } else {
    card.appendChild(el('<label class="f">Email<input id="ac-email" type="email" autocomplete="email" inputmode="email" placeholder="you@example.com"></label>'));
    card.appendChild(el('<button class="btn" id="ac-send">'+(acct.sent?"Send again":"Email me a sign-in link")+'</button>'));
    if(acct.sent) card.appendChild(el('<div class="line"><span class="muted">Link sent to '+esc(acct.sent)+'. Open it on this phone to sign in.</span></div>'));
    hint.textContent = "Optional. Sign in to keep your list on several phones and share it with your household. Without an account, everything stays on this phone.";
  }
  const q = id => s.querySelector("#"+id), keep = (id,k) => { const i=q(id); if(i){ i.value = acct.form[k]||(k==="email"?acct.sent||"":""); i.oninput = () => acct.form[k] = i.value; } };
  keep("ac-email","email"); keep("ac-code","code"); keep("ac-join","join");
  q("ac-send") && (q("ac-send").onclick = () => sendLink(q("ac-email").value));
  q("ac-verify") && (q("ac-verify").onclick = () => verifyCode(q("ac-code").value));
  q("ac-out") && (q("ac-out").onclick = signOut);
  q("ac-copy") && (q("ac-copy").onclick = copyCode);
  q("ac-joinb") && (q("ac-joinb").onclick = () => joinHousehold(q("ac-join").value));
  q("ac-del") && (q("ac-del").onclick = deleteAccount);
  return s;
}

/* ---------- Boot ---------- */
(function boot(){
  if(!sbReady()){ render(); return; }
  sb.auth.onAuthStateChange((ev, session) => {
    setTimeout(() => {
      if(session && session.user){ acct.sent = null; acct.form = {}; connect(session.user); }
      else if(ev==="SIGNED_OUT" && acct.user){ resetLocal(); }
    }, 0);
  });
  render();
})();
