/* Single-file ops console (no build step). Served at GET /ops. */
export const OPS_HTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sync Ops Console</title>
<style>
:root{--bg:#f6f7f9;--card:#fff;--fg:#1b1f24;--mut:#667085;--line:#e4e7ec;--acc:#b4235a;--ok:#12805c;--bad:#c4320a}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#171a21;--fg:#e6e8eb;--mut:#98a2b3;--line:#2a2f3a;--acc:#f06292;--ok:#4cc38a;--bad:#ff7a59}}
*{box-sizing:border-box}body{margin:0;font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);color:var(--fg)}
header{padding:16px;border-bottom:1px solid var(--line);display:flex;gap:12px;align-items:center;flex-wrap:wrap}
h1{font-size:17px;margin:0}h2{font-size:15px;margin:0 0 10px}main{padding:16px;display:grid;gap:16px;max-width:1200px;margin:0 auto}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;overflow:auto}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}
.kpi b{display:block;font-size:22px}.kpi span{color:var(--mut);font-size:12px}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--mut);font-weight:600}td.err{white-space:normal;max-width:380px;color:var(--bad)}
button{background:var(--acc);color:#fff;border:0;border-radius:6px;padding:5px 10px;cursor:pointer;font-size:12px}
input{padding:6px 8px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--fg)}
.pos{color:var(--ok)}.neg{color:var(--bad)}.mut{color:var(--mut)}.pill{font-size:12px;color:var(--mut)}
</style></head><body>
<header><h1>Order &amp; Inventory Sync · Ops</h1><span class="pill" id="upd"></span></header>
<main>
<section class="card"><h2>Day-one signals</h2><div class="kpis" id="kpis"></div><div id="push" style="margin-top:12px"></div></section>
<section class="card"><h2>Failed syncs</h2><div id="fail"></div></section>
<section class="card"><h2>SKU audit trail</h2>
<form id="f"><input id="sku" placeholder="e.g. MRS-LIP-104" required> <button>Look up</button></form><div id="audit" style="margin-top:12px"></div></section>
</main>
<script>
const $=s=>document.querySelector(s);const esc=v=>String(v??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const t=(rows,cols,extra)=>rows.length?'<table><tr>'+cols.map(c=>'<th>'+c+'</th>').join('')+(extra?'<th></th>':'')+'</tr>'+rows.map(r=>'<tr>'+cols.map(c=>'<td'+(c==='last_error'?' class="err"':'')+'>'+esc(r[c])+'</td>').join('')+(extra?'<td>'+extra(r)+'</td>':'')+'</tr>').join('')+'</table>':'<p class="mut">None</p>';
async function j(u,o){const r=await fetch(u,o);if(!r.ok)throw new Error(await r.text());return r.json()}
async function retry(u){try{await j(u,{method:'POST'});load()}catch(e){alert(e.message)}}
async function load(){
 const m=await j('/ops/metrics');
 const k=[['Oversell rate (24h)',(m.oversell_rate_24h*100).toFixed(2)+'%',m.oversell_rate_24h>0.003],
  ['Orders (24h)',m.orders_24h.total],['ERP backlog',m.erp_outbox.backlog,m.erp_outbox.backlog>0],
  ['ERP oldest (s)',m.erp_outbox.oldest_age_s,m.erp_outbox.oldest_age_s>900],
  ['Dead letters',m.dead_letters.inbound_events+m.dead_letters.erp+m.dead_letters.push_failed,(m.dead_letters.inbound_events+m.dead_letters.erp)>0],
  ['Inbound pending',m.inbound_pending]];
 $('#kpis').innerHTML=k.map(([a,b,bad])=>'<div class="kpi"><b class="'+(bad?'neg':'')+'">'+b+'</b><span>'+a+'</span></div>').join('');
 $('#push').innerHTML=t(m.stock_push,['channel','backlog','oldest_dirty_ms','p95_lag_ms','failed']);
 const f=await j('/ops/failures');
 $('#fail').innerHTML='<h3 class="pill">Inbound events</h3>'+t(f.inbound_events,['id','channel','event_type','external_order_id','attempts','last_error'],r=>'<button onclick="retry(\\'/ops/retry/event/'+r.id+'\\')">Retry</button>')
  +'<h3 class="pill">ERP</h3>'+t(f.erp,['id','kind','channel','external_order_id','attempts','last_error'],r=>'<button onclick="retry(\\'/ops/retry/erp/'+r.id+'\\')">Retry</button>')
  +'<h3 class="pill">Stock push</h3>'+t(f.stock_push,['sku','channel','attempts','last_error'],r=>'<button onclick="retry(\\'/ops/retry/push/'+encodeURIComponent(r.sku)+'/'+r.channel+'\\')">Retry</button>');
 $('#upd').textContent='updated '+new Date().toLocaleTimeString();
}
$('#f').onsubmit=async e=>{e.preventDefault();try{const d=await j('/ops/sku/'+encodeURIComponent($('#sku').value.trim()));const i=d.inventory;
 $('#audit').innerHTML='<p><b>'+esc(i.sku)+'</b> '+esc(i.name)+' · on hand <b>'+i.on_hand+'</b> · reserved <b>'+i.reserved+'</b> · available <b>'+i.available+'</b> · buffer '+i.safety_buffer+'</p>'
 +t(d.channels,['channel','last_pushed_qty','last_pushed_at','dirty','status'])+'<br>'
 +t(d.ledger.map(r=>({...r,when:new Date(r.created_at).toLocaleString(),order:r.channel?r.channel+' '+r.external_order_id:''})),['when','reason','delta_on_hand','delta_reserved','on_hand_after','reserved_after','actor','order','note'])}
 catch(err){$('#audit').innerHTML='<p class="neg">'+esc(err.message)+'</p>'}};
load();setInterval(load,5000);
</script></body></html>`;
