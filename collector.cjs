const fs = require('node:fs');
const path = require('node:path');
const {pipeline} = require('node:stream/promises');
const {Readable} = require('node:stream');
const {chromium} = require('playwright-core');
const {parseInput,CommentStore,findVideo,videoUrls,allowedMedia,writeCsv,atomicJson,columns} = require('./core.cjs');
const {readRows,space,parts}=require('./model.cjs');
const {saveImages}=require('./assets.cjs');

class Collector {
  constructor(base, notify) { this.base=base; this.notify=notify; this.context=null; this.active=null; this.launching=null; }
  async browser() {
    if (this.context) return this.context;
    if (this.launching) return this.launching;
    this.launching = chromium.launchPersistentContext(path.join(this.base,'browser-profile'), {
      channel:'msedge', headless:false, viewport:null, chromiumSandbox:true,
      args:['--start-maximized'], acceptDownloads:false
    }).then(ctx=> { this.context=ctx; ctx.on('close',()=>{this.context=null;}); return ctx; }).finally(()=>{this.launching=null;});
    return this.launching;
  }
  async login() {
    const ctx=await this.browser(); const page=ctx.pages()[0] || await ctx.newPage();
    if (!page.url().includes('douyin.com')) await page.goto('https://www.douyin.com/',{waitUntil:'domcontentloaded',timeout:45000});
    await page.bringToFront();
  }
  update(job, patch={}) {Object.assign(job,patch,{updatedAt:new Date().toISOString()}); atomicJson(path.join(job.folder,'task.json'),job); this.notify(job); }
  log(job,message) { job.messages = [...(job.messages||[]).slice(-39),{time:new Date().toLocaleTimeString('zh-CN'),text:message}]; this.update(job); }
  pause() { if (this.active) {this.active.paused=true; this.update(this.active.job,{status:'paused',message:'已暂停，已采集数据保留。'});} }
  resume() {if(this.active) {this.active.paused=false;this.active.idle=Date.now();this.update(this.active.job,{status:'running',message:'继续采集'});} }
  stop() {if(this.active){this.active.stopped=true;this.active.abort.abort();}}
  async execute(job,part='all',settings={}) {
    if(this.active||this.browsing)throw new Error('浏览器正在处理其他任务。');
    const parsed=parseInput(job.url||job.input);
    job.parts=parts(job);job.failure=null;job.exportWarning=null;job.videoError=null;
    const state={job,part,settings,stopped:false,paused:false,idle:Date.now(),abort:new AbortController(),rootEnd:false,store:null,detail:null,media:new Set(),pending:new Set(),videoAttempted:false,videoPromise:null,replyEnds:new Set(),progressKey:'',lastAdvance:Date.now()};
    state.collectComments=job.comments&&['all','comments'].includes(part)&&job.parts.comments!=='completed';
    state.collectVideo=job.video&&['all','video'].includes(part)&&(!job.videoSaved||!fs.existsSync(path.join(job.folder,'video.mp4')));
    if(part==='video')state.collectVideo=true;
    this.active=state;
    try{await this.run(state,parsed);}finally{this.active=null;}
    return job;
  }
  async start(job) {
    if(this.active) throw new Error('已有任务正在运行，请先完成或停止。');
    const parsed=parseInput(job.input);
    const state={job,stopped:false,paused:false,idle:Date.now(),abort:new AbortController(),rootEnd:false,store:null,detail:null,media:new Set(),pending:new Set(),videoAttempted:false,videoPromise:null};
    this.active=state;
    this.run(state,parsed).catch(e=>{this.update(job,{status:job.commentCount||job.videoSaved?'partial':'failed',message:e.message});}).finally(()=>{this.active=null;});
    return job;
  }
  async run(s,parsed) {
    const job=s.job; let page; let listener;
    try {
      job.parts=parts(job);
      s.collectComments ??=job.comments; s.collectVideo ??=job.video&&!job.videoSaved;s.replyEnds??=new Set();s.settings??={};
      s.store=new CommentStore(parsed.id||job.videoId,job.limit,job.replies);
      for(const row of readRows(job.folder))s.store.rows.set(row.id,row);
      job.commentCount=s.store.rows.size;job.rootCount=[...s.store.rows.values()].filter(r=>r.level===1).length;job.replyCount=job.commentCount-job.rootCount;
      this.update(job,{status:'running',message:s.part==='images'?'正在处理评论图片…':'正在恢复任务…'});
      // Rewrite a valid journal before appending: an interrupted last line must not corrupt the next page.
      if(job.comments)fs.writeFileSync(path.join(job.folder,'comments.jsonl'),[...s.store.rows.values()].map(r=>JSON.stringify(r)+'\n').join(''));
      if(s.part==='images')return;
      if(!s.collectComments&&!s.collectVideo)return;
      if(s.collectComments)job.parts.comments='running';if(s.collectVideo)job.parts.video='running';
      this.update(job,{status:'running',message:'正在打开抖音作品…'});
      if(space(job.folder)<200*1024*1024)throw new Error('磁盘空间不足 200 MB，请释放空间或更改保存位置。');
      const ctx=await this.browser(); page=ctx.pages()[0] || await ctx.newPage();
      if (!parsed.id) {
        await page.goto(parsed.url,{waitUntil:'domcontentloaded',timeout:45000});
        for(let n=0;n<20 && !parsed.id;n++){try{parsed=parseInput(page.url());}catch{} if(!parsed.id) await new Promise(r=>setTimeout(r,500));}
        if(!parsed.id) throw new Error('短链未能解析为视频，请复制浏览器中的完整视频链接后重试。');
      }
      job.videoId=parsed.id; job.url=parsed.url;
      s.store.videoId=parsed.id;
      listener=response=>{
        const p=this.consume(s,response).catch(()=>{}); s.pending.add(p); p.finally(()=>s.pending.delete(p));
      };
      page.on('response',listener);
      for(let attempt=0;attempt<=Number(s.settings.retries??2);attempt++){try{await page.goto(parsed.url,{waitUntil:'domcontentloaded',timeout:30000});break;}catch(e){if(attempt===Number(s.settings.retries??2)||s.stopped)throw e;this.log(job,'页面加载失败，正在重试…');await new Promise(r=>setTimeout(r,1500));}}
      s.idle=Date.now();
      this.log(job,'已打开作品。需要时请在 Edge 窗口登录或完成验证，工具会保留已有结果。');
      let round=0;
      while(!s.stopped) {
        if(page.isClosed()) throw new Error('采集浏览器已关闭；已有结果已保存。');
        if(s.paused){await new Promise(r=>setTimeout(r,500));continue;}
        if(round++ % 4===0) await this.readPage(s,page);
        if(s.collectVideo && !s.videoAttempted && s.media.size) {
          s.videoAttempted=true;
          s.videoPromise=this.download(s,ctx).catch(e=>{this.update(job,{videoError:s.stopped?'下载已停止':e.message});});
        }
        const commentsFinished=!s.collectComments || s.store.rows.size>=job.limit || this.commentsExhausted(s);
        const videoFinished=!s.collectVideo || job.videoSaved || job.videoError;
        if(commentsFinished && videoFinished) break;
        if(commentsFinished && s.videoPromise) {await s.videoPromise;break;}
        if(Date.now()-s.idle>45000) {
          s.paused=true;
          const reason=await this.inspectPage(page);
          if(reason.code==='unavailable')throw new Error(reason.message);
          this.update(job,{status:'needs_action',issue:reason.code,message:reason.message});
          continue;
        }
        if(s.collectComments && !commentsFinished) await this.advance(page,job.replies,round);
        await new Promise(r=>setTimeout(r,Number(s.settings.interval||2)*1000));
      }
      if(s.videoPromise) {this.update(job,{message:s.stopped?'正在保存结果…':'正在完成视频下载…'});await s.videoPromise;}
    } catch(e) {job.failure=e.message;}
    finally {
      if(page && listener) page.off('response',listener);
      await Promise.allSettled([...s.pending]);
      if(s.videoPromise) {if(job.failure)s.abort.abort();await s.videoPromise;}
      if(job.downloadImages&&['all','comments','images',undefined].includes(s.part)&&!s.stopped){job.parts.images='running';this.update(job,{message:'正在保存评论图片…'});const result=await saveImages(job,[...(s.store?.rows.values()||[])],patch=>this.update(job,patch),()=>s.stopped);job.parts.images=result.failed||result.interrupted?'partial':'completed';}
      try{await this.export(s);}catch(e){job.exportWarning='导出未完成：'+e.message;}
      const count=s.store?.rows.size||0;
      const commentsOk=!job.comments || job.parts.comments==='completed' || count>=job.limit || this.commentsExhausted(s);
      const videoOk=!job.video || job.videoSaved;
      if(job.comments)job.parts.comments=commentsOk?'completed':count?'partial':'failed';
      if(job.video)job.parts.video=videoOk?'completed':'failed';
      const complete=!s.stopped&&!job.failure&&!job.videoError&&!job.exportWarning&&commentsOk&&videoOk&&(!job.downloadImages||job.parts.images==='completed');
      this.update(job,{status:complete?'completed':(count||job.videoSaved?'partial':s.stopped?'cancelled':'failed'),
        message:complete?(job.comments&&count>=job.limit?'已达到设定条数，文件已保存。':'已保存本次页面返回的结果。'):job.failure||job.videoError||job.exportWarning||(s.stopped?'已结束，现有结果已保存，可在原任务继续。':'部分项目未完成，可单独重试。'),
        stopReason:s.stopped?'user_stopped':count>=job.limit?'limit_reached':s.rootEnd?'root_endpoint_exhausted':job.failure?'error':'incomplete', finishedAt:new Date().toISOString()});
    }
  }
  async consume(s,response) {
    if(s.stopped||(s.paused&&s.job.status!=='needs_action')) return;
    const u=new URL(response.url());
    if(!u.hostname.endsWith('.douyin.com') && u.hostname!=='douyin.com') return;
    if(u.pathname.includes('/comment/list')) {
      if(!(s.collectComments??s.job.comments)) return;
      const id=u.searchParams.get('aweme_id')||u.searchParams.get('item_id');
      if(id!==s.job.videoId) return;
      const data=await response.json();
      if(s.stopped||(s.paused&&s.job.status!=='needs_action')) return;
      if(data.status_code && data.status_code!==0) {this.log(s.job,'评论接口暂未返回正常数据，请检查浏览器中的登录或验证提示。');return;}
      if(!Array.isArray(data.comments)) return;
      const isReply=u.pathname.includes('/reply/');
      s.replyEnds??=new Set();
      if(!isReply && data.has_more===0) s.rootEnd=true;
      if(isReply&&data.has_more===0)s.replyEnds.add(u.searchParams.get('comment_id'));
      const progressKey=u.pathname+':'+u.searchParams.get('cursor');
      if(s.progressKey!==progressKey&&data.comments.length){s.idle=Date.now();s.progressKey=progressKey;}
      s.job.checkpoint={rootEnd:s.rootEnd,lastCursor:u.searchParams.get('cursor'),lastPageType:isReply?'reply':'root',savedAt:new Date().toISOString(),strategy:'replay-and-deduplicate'};
      const rows=s.store.add(data.comments,isReply?u.searchParams.get('comment_id')||'':'');
      if(rows.length) {
        fs.appendFileSync(path.join(s.job.folder,'comments.jsonl'),rows.map(r=>JSON.stringify(r)).join('\n')+'\n');
        s.idle=Date.now();
        if(s.job.status==='needs_action')s.paused=false;
        this.update(s.job,{status:'running',commentCount:s.store.rows.size,rootCount:[...s.store.rows.values()].filter(r=>r.level===1).length,replyCount:[...s.store.rows.values()].filter(r=>r.level===2).length,preview:[...s.store.rows.values()].slice(-30),message:`已采集 ${s.store.rows.size} 条评论，逐页保存中…`});
      }
    } else if(u.pathname.includes('/aweme/detail/')) {
      const data=await response.json(); this.setDetail(s,findVideo(data,s.job.videoId));
    }
  }
  setDetail(s,d) {
    if(!d) return; s.detail=d;
    for(const url of videoUrls(d)) if(allowedMedia(url)) s.media.add(url);
    this.update(s.job,{title:d.desc||s.job.title,author:d.author?.nickname||'',cover:d.video?.cover?.url_list?.[0]||s.job.cover||'',pageCommentCount:d.statistics?.comment_count ?? null});
  }
  async readPage(s,page) {
    const embedded=await page.evaluate(()=>window._ROUTER_DATA || null).catch(()=>null);
    if(embedded)this.setDetail(s,findVideo(embedded,s.job.videoId));
    const contents=await page.locator('script#RENDER_DATA, script#__NEXT_DATA__, script[type="application/json"]').allTextContents().catch(()=>[]);
    for(const text of contents){try{this.setDetail(s,findVideo(JSON.parse(text.startsWith('%')?decodeURIComponent(text):text),s.job.videoId));}catch{}}
    const media=await page.locator('video').evaluateAll(videos=>{
      const visible=videos.filter(v=>{const r=v.getBoundingClientRect();return r.width>150&&r.height>150&&r.bottom>0&&r.right>0&&r.top<innerHeight&&r.left<innerWidth&&getComputedStyle(v).visibility!=='hidden';});
      return visible.length===1?[visible[0].currentSrc||visible[0].src]:[];
    }).catch(()=>[]);
    for(const url of media) if(allowedMedia(url)) s.media.add(url);
    if(!s.job.title){const title=await page.title();if(title&&!/^抖音[ -]/.test(title))this.update(s.job,{title});}
  }
  async advance(page,replies,round) {
    // Only interact with comment controls. No likes, follows, messages or verification automation.
    if(replies) {
      const expand=page.getByText(/^(展开\s*\d*\s*条?回复|查看全部\s*\d*\s*条?回复|展开更多回复|查看更多回复)$/).first();
      if(await expand.isVisible().catch(()=>false)) {await expand.click({timeout:1500}).catch(()=>{});return;}
    }
    const area=page.locator('[data-e2e="comment-list"], [data-e2e="comment-list-container"]').first();
    if(await area.isVisible().catch(()=>false)) {
      await area.evaluate(el=>{
        let target=el;
        for(let p=el;p&&p!==document.body;p=p.parentElement){if(p.scrollHeight>p.clientHeight+50 && /auto|scroll/.test(getComputedStyle(p).overflowY)){target=p;break;}}
        target.scrollBy(0,Math.max(400,target.clientHeight*.7));
      }).catch(()=>{});
      await area.hover({timeout:1000}).catch(()=>{}); await page.mouse.wheel(0,600).catch(()=>{});
    } else {
      const button=page.locator('[data-e2e="video-comment"]').first();
      if(round%5===1 && await button.isVisible().catch(()=>false)) await button.click({timeout:1500}).catch(()=>{});
    }
  }
  async download(s,ctx) {
    const job=s.job; this.log(job,'正在保存视频文件…');
    let last='未取得可用的视频下载地址。';
    for(const candidate of Array.from({length:Number(s.settings?.retries??2)+1},()=>[...s.media]).flat()) {
      if(s.stopped) break;
      const tmp=path.join(job.folder,'video.mp4.part');
      try {
        let url=candidate; let response;
        const timeout=AbortSignal.timeout(180000); const signal=AbortSignal.any([s.abort.signal,timeout]);
        for(let redirect=0;redirect<6;redirect++) {
          if(!allowedMedia(url)) throw new Error('视频地址不在支持的抖音媒体域名范围。');
          const cookies=await ctx.cookies(url);
          response=await fetch(url,{redirect:'manual',signal,headers:{'User-Agent':'Mozilla/5.0','Referer':job.url,'Cookie':cookies.map(c=>`${c.name}=${c.value}`).join(';')}});
          if(response.status>=300&&response.status<400){const location=response.headers.get('location');await response.body?.cancel();if(!location)throw new Error('视频重定向缺少地址。');url=new URL(location,url).href;continue;}break;
        }
        if(!response.ok) {await response.body?.cancel();throw new Error(`视频服务器返回 ${response.status}`);}
        const type=response.headers.get('content-type')||'';
        if(!/video|octet-stream/.test(type)){await response.body?.cancel();throw new Error('下载地址返回的不是视频文件。');}
        const total=Number(response.headers.get('content-length'))||0;let bytes=0,lastNotify=0;
        if(space(job.folder)<Math.max(total,50*1024*1024)+100*1024*1024){await response.body?.cancel();throw new Error('磁盘空间不足，视频下载已停止。');}
        const stream=Readable.fromWeb(response.body);
        stream.on('data',chunk=>{bytes+=chunk.length;if(Date.now()-lastNotify>1000){lastNotify=Date.now();this.update(job,{videoBytes:bytes,videoTotal:total});}});
        await pipeline(stream,fs.createWriteStream(tmp),{signal});
        const fd=fs.openSync(tmp,'r');const head=Buffer.alloc(32);fs.readSync(fd,head,0,32,0);fs.closeSync(fd);
        if(bytes<1024 || !head.includes(Buffer.from('ftyp')) || (total && bytes!==total)) throw new Error('视频完整性检查失败，保留为临时文件。');
        fs.renameSync(tmp,path.join(job.folder,'video.mp4'));
        job.parts??=parts(job);job.parts.video='completed';this.update(job,{videoSaved:true,videoBytes:bytes,videoTotal:total,videoError:null}); return;
      }catch(e){last=e.message;if(s.stopped||/磁盘空间/.test(last))break;await new Promise(r=>setTimeout(r,1000));}
    }
    throw new Error(last);
  }
  async export(s) {
    const job=s.job;const rows=[...(s.store?.rows.values()||[])];
    if(job.comments){
      writeCsv(path.join(job.folder,'comments.csv'),rows);
      atomicJson(path.join(job.folder,'comments.json'),rows);
      try {
        const ExcelJS=require('exceljs');const book=new ExcelJS.Workbook();const sheet=book.addWorksheet('评论');
        sheet.columns=columns.map(([key,header])=>({key,header,width:key==='text'?65:22}));
        for(const row of rows) sheet.addRow({...row,createdAt:row.createdAt?new Date(row.createdAt).toLocaleString('sv-SE',{timeZone:'Asia/Shanghai'}):'',images:(row.images||[]).join('\n')});
        sheet.getCell('H1').value='发布时间（北京时间 UTC+8）';
        sheet.views=[{state:'frozen',ySplit:1}];sheet.autoFilter={from:'A1',to:'L1'};
        sheet.getRow(1).font={bold:true,color:{argb:'FFFFFFFF'}};sheet.getRow(1).fill={type:'pattern',pattern:'solid',fgColor:{argb:'FF183B43'}};
        sheet.getColumn('text').alignment={wrapText:true,vertical:'top'};
        const excelFile=path.join(job.folder,'comments.xlsx');await book.xlsx.writeFile(excelFile+'.tmp');fs.renameSync(excelFile+'.tmp',excelFile);
      }catch(e){job.exportWarning='Excel 导出失败，CSV 和 JSON 已保存：'+e.message;}
    }
    atomicJson(path.join(job.folder,'video.json'),{id:job.videoId,url:job.url,title:job.title,author:job.author,pageCommentCount:job.pageCommentCount,capturedAt:new Date().toISOString(),videoSaved:!!job.videoSaved});
  }
  async close(){this.stop();if(this.context)await this.context.close();}
  commentsExhausted(s){if(!s.rootEnd)return false;if(!s.job.replies)return true;const rows=[...(s.store?.rows.values()||[])];return rows.filter(r=>r.level===1).every(root=>!root.replyCount||s.replyEnds?.has(root.id)||rows.filter(r=>r.rootId===root.id).length>=root.replyCount);}
  async inspectPage(page){const text=await page.locator('body').innerText({timeout:3000}).catch(()=>'');if(/作品不存在|视频已删除|暂时无法观看|作品已删除|私密作品/.test(text))return {code:'unavailable',message:'该作品已删除、设为私密或当前账号无法访问。'};if(/拖动.*滑块|请完成.*验证|安全验证|验证码验证/.test(text))return {code:'verification',message:'请在 Edge 窗口完成安全验证，然后继续。'};if(/登录后.*评论|登录后.*查看|登录后.*浏览/.test(text))return {code:'login',message:'请在 Edge 窗口登录抖音，然后继续。'};return {code:'no_data',message:'暂未收到新数据。请检查网络或在 Edge 中展开/滚动评论后继续；也可以结束并保存。'};}
  async loginStatus(){const ctx=await this.browser();const cookies=await ctx.cookies('https://www.douyin.com');return {hasSession:cookies.some(c=>['sessionid','sessionid_ss','sid_tt'].includes(c.name)),message:cookies.some(c=>['sessionid','sessionid_ss','sid_tt'].includes(c.name))?'检测到登录凭据；实际可用性以采集结果为准。':'未检测到登录凭据，请在 Edge 中登录。'};}
  async logout(){if(this.active||this.browsing)throw new Error('请先暂停队列并结束当前操作。');const ctx=await this.browser();await ctx.clearCookies();const p=ctx.pages()[0]||await ctx.newPage();const session=await ctx.newCDPSession(p);await session.send('Storage.clearDataForOrigin',{origin:'https://www.douyin.com',storageTypes:'all'});await session.detach();await ctx.close();return '已清除专用浏览器中的抖音登录状态。';}
}
module.exports={Collector};
