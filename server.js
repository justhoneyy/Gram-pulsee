import http from'node:http';import fs from'node:fs';
/* Database: Postgres when DATABASE_URL is set (Render / Vercel), else local SQLite file */
const PG=process.env.DATABASE_URL;let run;
if(PG){const{default:pg}=await import('pg');const pool=new pg.Pool({connectionString:PG,max:process.env.VERCEL?1:5,ssl:/localhost|127\.0\.0\.1/.test(PG)?false:{rejectUnauthorized:false}});
 run=async(s,a=[])=>{let i=0;return(await pool.query(s.replace(/\?/g,()=>'$'+ ++i),a)).rows}}
else{const{DatabaseSync}=await import('node:sqlite');const db=new DatabaseSync(process.env.DB||'gram.db');
 run=async(s,a=[])=>{const st=db.prepare(s);if(/select|returning/i.test(s))return st.all(...a);st.run(...a);return[]}}
const ID=PG?'serial primary key':'integer primary key autoincrement';
const ready=(async()=>{
 await run(`create table if not exists assets(id ${ID},name text not null,type text,ward int,cond int,pop int,cost double precision,dep int default 0)`);
 await run(`create table if not exists reports(id ${ID},name text,cat text,ward int,asset int,text text,ts timestamp default current_timestamp)`);
 if(!(await run('select 1 from assets limit 1')).length)for(const a of[['Water Tank W02','Water',1,55,900,2.5,0],['Drain D04','Drainage',4,28,340,1.8,0],['Road R07','Road',4,40,520,4.2,2],['School Access S02','Education',4,62,210,1.1,3],['Health Centre H01','Health',2,71,1200,3,3],['Hand Pump P03','Water',3,35,180,.6,1],['Toilet Block T01','Sanitation',5,48,260,.9,1]])await run('insert into assets(name,type,ward,cond,pop,cost,dep)values(?,?,?,?,?,?,?)',a)})();
const levels=(id,a)=>{const seen=new Map([[id,0]]);let q=[id];while(q.length){const n=[];for(const p of q)for(const x of a)if(x.dep===p&&!seen.has(x.id)){seen.set(x.id,seen.get(p)+1);n.push(x.id)}q=n}seen.delete(id);return seen};
const rank=async()=>{const a=await run('select * from assets');return a.map(x=>{const k=[...levels(x.id,a).keys()].map(i=>a.find(y=>y.id===i));const aff=k.reduce((s,y)=>s+y.pop,x.pop);return{...x,dependents:k.length,affected:aff,score:Math.round((100-x.cond)*.6+Math.min(aff,2000)/50+k.length*4),risk:x.cond<40?'High':x.cond<60?'Medium':'Low'}}).sort((a,b)=>b.score-a.score)};
const A=b=>{const name=String(b.name||'').trim().slice(0,80);if(!name)throw 400;const n=(v,lo,hi)=>Math.min(hi,Math.max(lo,+v||0));return[name,String(b.type||'Road').slice(0,20),n(b.ward,1,6)||1,n(b.cond,0,100),n(b.pop,0,1e6),n(b.cost,0,1e4),Math.trunc(+b.dep)||0]};
const body=r=>new Promise(ok=>{let d='';r.on('data',c=>d+=c);r.on('end',()=>{try{ok(JSON.parse(d||'{}'))}catch{ok({})}})});
let page;
export default async function handler(q,s){const p=new URL(q.url,'http://x').pathname,m=q.method,send=(o,c=200)=>{s.writeHead(c,{'content-type':'application/json'});s.end(JSON.stringify(o))};
try{
if(p==='/healthz')return send({ok:1});
if(p==='/'||p==='/index.html'){page??=fs.readFileSync(new URL('./index.html',import.meta.url));s.writeHead(200,{'content-type':'text/html; charset=utf-8'});return s.end(page)}
await ready;
const id=+p.split('/')[3]||0,b=m==='GET'||m==='DELETE'?{}:await body(q);
if(p==='/api/summary'){const a=await rank();return send({pulse:Math.round(a.reduce((t,x)=>t+x.cond,0)/(a.length||1)),assets:a.length,high:a.filter(x=>x.risk==='High').length,people:a.reduce((t,x)=>t+x.pop,0),reports:Number((await run('select cast(count(*) as integer) c from reports'))[0].c),alerts:a.slice(0,3)})}
if(p==='/api/assets'&&m==='GET')return send(await rank());
if(p==='/api/assets'&&m==='POST')return send({id:(await run('insert into assets(name,type,ward,cond,pop,cost,dep)values(?,?,?,?,?,?,?) returning id',A(b)))[0].id},201);
if(p.startsWith('/api/assets/')&&m==='PUT'){await run('update assets set name=?,type=?,ward=?,cond=?,pop=?,cost=?,dep=? where id=?',[...A(b),id]);return send({ok:1})}
if(p.startsWith('/api/assets/')&&m==='DELETE'){await run('update assets set dep=0 where dep=?',[id]);await run('delete from assets where id=?',[id]);return send({ok:1})}
if(p==='/api/reports'&&m==='GET')return send(await run('select r.*,a.name asset_name from reports r left join assets a on a.id=r.asset order by r.id desc limit 30'));
if(p==='/api/reports'&&m==='POST'){const t=String(b.text||'').trim().slice(0,800);if(t.length<8)throw 400;await run('insert into reports(name,cat,ward,asset,text)values(?,?,?,?,?)',[String(b.name||'Anonymous').slice(0,60),String(b.cat||'Other').slice(0,20),+b.ward||1,+b.asset||0,t]);if(+b.asset)await run('update assets set cond=case when cond>3 then cond-3 else 0 end where id=?',[+b.asset]);return send({ok:1},201)}
if(p.startsWith('/api/reports/')&&m==='DELETE'){await run('delete from reports where id=?',[id]);return send({ok:1})}
if(p==='/api/simulate'&&m==='POST'){const a=await rank(),root=a.find(x=>x.id===+b.id);if(!root)throw 404;const L=levels(root.id,a),steps=[...L].map(([i,l])=>({...a.find(x=>x.id===i),level:l})).sort((x,y)=>x.level-y.level);return send({root,steps,people:root.affected})}
if(p==='/api/budget'&&m==='POST'){const W=Math.min(200,Math.round((+b.budget||0)*10)),it=(await rank()).filter(x=>x.cond<70&&x.cost>0).map(x=>({...x,u:Math.max(1,Math.round(x.cost*10))}));
const dp=Array.from({length:it.length+1},()=>new Array(W+1).fill(0));it.forEach((x,i)=>{for(let w=0;w<=W;w++)dp[i+1][w]=Math.max(dp[i][w],w>=x.u?dp[i][w-x.u]+x.score:0)});
const pick=[];for(let i=it.length,w=W;i>0;i--)if(dp[i][w]!==dp[i-1][w]){pick.push(it[i-1]);w-=it[i-1].u}
const basic=[];let left=W;for(const x of[...it].sort((x,y)=>x.cost-y.cost))if(x.u<=left){basic.push(x);left-=x.u}
const sum=(l,k)=>Math.round(l.reduce((t,x)=>t+x[k],0)*10)/10;
return send({plan:pick,spent:sum(pick,'cost'),benefit:sum(pick,'score'),people:sum(pick,'affected'),basic:{n:basic.length,spent:sum(basic,'cost'),benefit:sum(basic,'score')}})}
send({error:'Not found'},404)}catch(e){console.error(e);const c=typeof e==='number';send({error:c?'Invalid or missing data':'Server error'},c?e:500)}}
if(!process.env.VERCEL){const port=process.env.PORT||3000;http.createServer(handler).listen(port,()=>console.log('GRAM-PULSE running on port '+port))}
