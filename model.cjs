const fs=require('node:fs');const path=require('node:path');const {randomUUID}=require('node:crypto');
const {parseInput,atomicJson,safeName}=require('./core.cjs');
function batch(text){const found=String(text||'').match(/https?:\/\/[^\s<>"“”]+/g)||[];const items=[],errors=[],seen=new Set();let duplicates=0;for(const input of found){try{const p=parseInput(input);const key=p.id||p.url;if(seen.has(key)){duplicates++;continue;}seen.add(key);items.push({...p,input});}catch(e){errors.push({input,error:e.message});}}return {items,errors,duplicates};}
function readRows(folder){const file=path.join(folder,'comments.jsonl');if(!fs.existsSync(file)){try{return JSON.parse(fs.readFileSync(path.join(folder,'comments.json'),'utf8'));}catch{return [];}}
 const text=fs.readFileSync(file,'utf8');const rows=new Map();const lines=text.split('\n');for(const [i,line] of lines.entries()){if(!line.trim())continue;try{const row=JSON.parse(line);if(row.id)rows.set(row.id,row);}catch{if(i<lines.length-2)throw new Error('评论数据损坏，请保留文件并导出诊断信息。');}}return [...rows.values()];}
function queryRows(rows,f={}){
 const words=String(f.keyword||'').trim().toLowerCase().split(/\s+/).filter(Boolean);const from=f.from?Date.parse(f.from+'T00:00:00+08:00'):-Infinity;const to=f.to?Date.parse(f.to+'T23:59:59.999+08:00'):Infinity;
 if(Number.isNaN(from)||Number.isNaN(to)||from>to)throw new Error('请检查开始和结束日期。');
 if(!Number.isFinite(Number(f.minLikes||0))||!Number.isFinite(Number(f.maxLikes||0))||Number(f.minLikes||0)<0||Number(f.maxLikes||0)<0||(f.maxLikes&&Number(f.maxLikes)<Number(f.minLikes||0)))throw new Error('点赞数范围不正确。');
 let out=rows.filter(r=>words.every(w=>r.text.toLowerCase().includes(w))&&(!f.region||r.region.includes(f.region))&&(!f.level||r.level===Number(f.level))&&Number(r.likes)>=Number(f.minLikes||0)&&(!f.maxLikes||Number(r.likes)<=Number(f.maxLikes))&&((!f.from&&!f.to)||(r.createdAt&&Date.parse(r.createdAt)>=from&&Date.parse(r.createdAt)<=to)));
 if(f.sort==='likes')out.sort((a,b)=>b.likes-a.likes);if(f.sort==='newest')out.sort((a,b)=>Date.parse(b.createdAt||0)-Date.parse(a.createdAt||0));if(f.sort==='oldest')out.sort((a,b)=>Date.parse(a.createdAt||0)-Date.parse(b.createdAt||0));
 return out;
}
function parts(job){return job.parts||{video:job.video?(job.videoSaved?'completed':'pending'):'skipped',comments:job.comments?(job.status==='completed'?'completed':'pending'):'skipped',images:job.downloadImages?'pending':'skipped'};}
function space(folder){fs.mkdirSync(folder,{recursive:true});const s=fs.statfsSync(folder);return Number(s.bavail)*Number(s.bsize);}
class Repository{
 constructor(root,defaultOutput){this.root=root;this.data=path.join(root,'data');this.file=path.join(this.data,'history.json');fs.mkdirSync(this.data,{recursive:true});this.jobs=[];this.settings={output:defaultOutput||path.join(root,'downloads'),interval:2,retries:2};
 try{Object.assign(this.settings,JSON.parse(fs.readFileSync(path.join(this.data,'settings.json'))));}catch{}
 if(fs.existsSync(this.file)){try{this.jobs=JSON.parse(fs.readFileSync(this.file));}catch{throw new Error('任务记录损坏，已保留原文件，请先修复或恢复备份。');}}
 for(const j of this.jobs){try{const disk=JSON.parse(fs.readFileSync(path.join(j.folder,'task.json')));if(disk.updatedAt>j.updatedAt)Object.assign(j,disk);}catch{}j.parts=parts(j);if(['running','paused','needs_action','stopping'].includes(j.status)){j.status='interrupted';j.message='上次运行中断，可在原任务继续；已保存评论会自动去重。';}j.preview=[];}
 this.save();
 }
 save(){atomicJson(this.file,this.jobs);}
 update(job,patch={}){Object.assign(job,patch,{updatedAt:new Date().toISOString()});atomicJson(path.join(job.folder,'task.json'),job);this.save();}
 get(id){const j=this.jobs.find(j=>j.id===id);if(!j)throw new Error('找不到任务。');return j;}
 create(items,o){const limit=Number(o.limit);if(!Number.isInteger(limit)||limit<1||limit>20000)throw new Error('评论上限为 1～20000 条。');if(!o.video&&!o.comments)throw new Error('至少选择一项采集内容。');if(space(this.settings.output)<200*1024*1024)throw new Error('保存位置剩余空间不足 200 MB，请更换目录。');
 const validated=items.map(item=>{const itemLimit=Number(item.limit??limit);if(!Number.isInteger(itemLimit)||itemLimit<1||itemLimit>20000)throw new Error('每个作品的采集上限必须是 1～20000 的整数。');return {item,itemLimit,p:parseInput(item.url||item.input)};});
 const created=[],skipped=[];for(const {item,itemLimit,p} of validated){if(this.jobs.some(j=>['queued','running','paused','needs_action'].includes(j.status)&&(j.videoId||j.url)===(p.id||p.url))){skipped.push(p.url);continue;}
 const id=randomUUID();const folder=path.join(this.settings.output,safeName(item.author||'未识别作者'),safeName(`${p.id||'短链'}_${item.title||'作品'}_${id.slice(0,8)}`));fs.mkdirSync(folder,{recursive:true});
 const job={id,input:p.url,url:p.url,videoId:p.id,title:item.title||'',author:item.author||'',cover:item.cover||'',pageCommentCount:item.pageCommentCount??null,folder,archiveRoot:this.settings.output,limit:itemLimit,video:!!o.video,comments:!!o.comments,replies:!!o.replies,downloadImages:!!o.comments&&!!o.downloadImages,status:'queued',commentCount:0,rootCount:0,replyCount:0,messages:[],preview:[],createdAt:new Date().toISOString()};job.parts=parts(job);this.jobs.unshift(job);this.update(job);created.push(job);}
 return {created,skipped};
 }
 configure(value){const interval=Number(value.interval),retries=Number(value.retries);if(!Number.isFinite(interval)||interval<1||interval>30||!Number.isInteger(retries)||retries<0||retries>5)throw new Error('间隔为 1～30 秒，重试为 0～5 次。');Object.assign(this.settings,{interval,retries});atomicJson(path.join(this.data,'settings.json'),this.settings);return this.settings;}
 archive(job){if(!job.archiveRoot||!job.videoId||!['completed','partial','failed','cancelled'].includes(job.status))return;const target=path.join(job.archiveRoot,safeName(job.author||'未识别作者'),safeName(`${job.videoId}_${job.title||'作品'}`).slice(0,55)+'_'+job.id.slice(0,8));if(path.resolve(target)===path.resolve(job.folder))return;try{fs.mkdirSync(path.dirname(target),{recursive:true});fs.renameSync(job.folder,target);job.folder=target;atomicJson(path.join(target,'task.json'),job);}catch(e){job.archiveWarning='文件保留在原目录：'+e.message;}}
}
class Queue{
 constructor(repo,collector,notify){this.repo=repo;this.collector=collector;this.notify=notify;this.running=false;this.enabled=false;}
 async pump(){if(this.running||!this.enabled||this.collector.active||this.collector.browsing)return;const j=[...this.repo.jobs].reverse().find(j=>j.status==='queued');if(!j)return;this.running=true;
 try{await this.collector.execute(j,j.retryPart||'all',this.repo.settings);}catch(e){this.repo.update(j,{status:'failed',message:e.message});}finally{delete j.retryPart;this.repo.save();this.running=false;this.notify();if(this.enabled)setTimeout(()=>this.pump(),500);}}
 start(){this.enabled=true;this.pump();this.notify();}
 pauseQueue(){this.enabled=false;this.notify();}
 retry(id,part='all'){if(!['all','video','comments','images'].includes(part))throw new Error('未知重试项目。');const j=this.repo.get(id);if(['queued','running','paused','needs_action'].includes(j.status))throw new Error('任务已经在队列中。');if(part==='video'&&!j.video||part==='comments'&&!j.comments||part==='images'&&!j.downloadImages)throw new Error('此任务没有选择该项目。');j.retryPart=part;this.repo.update(j,{status:'queued',failure:null,message:'已加入续采队列，保留原文件和已采集评论。'});this.start();}
 remove(id){const j=this.repo.get(id);if(j.status!=='queued')throw new Error('只能移除尚未开始的排队任务。');this.repo.update(j,{status:'cancelled',message:'已移出队列。'});this.notify();}
}
module.exports={batch,readRows,queryRows,parts,space,Repository,Queue};
