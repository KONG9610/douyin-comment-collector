const fs = require('node:fs');
const path = require('node:path');

function parseInput(input) {
  const match = String(input || '').match(/https?:\/\/[^\s<>"“”]+/i);
  if (!match) throw new Error('请粘贴抖音视频链接或包含链接的分享文案。');
  const url = new URL(match[0].replace(/[，。！、；）)]+$/, ''));
  if (url.protocol !== 'https:' || !['douyin.com', 'www.douyin.com', 'v.douyin.com', 'www.iesdouyin.com', 'v.iesdouyin.com', 'iesdouyin.com'].includes(url.hostname)) throw new Error('仅支持 HTTPS 抖音作品链接。');
  const id = url.pathname.match(/\/(?:video|note)\/(\d+)/)?.[1] || url.searchParams.get('modal_id') || url.searchParams.get('aweme_id');
  if (id && !/^\d{10,30}$/.test(id)) throw new Error('作品编号格式不正确。');
  if (!id && !['v.douyin.com', 'v.iesdouyin.com'].includes(url.hostname)) throw new Error('请使用单个视频的分享链接，不支持主页或搜索链接。');
  return { url: id ? `https://www.douyin.com/video/${id}` : url.href, id: id || null };
}
function safeName(value) {
  const result = String(value || '未命名').replace(/[<>:"/\\|?*\x00-\x1f]/g, '_').replace(/[. ]+$/, '').slice(0, 70);
  return /^(con|prn|aux|nul|com\d|lpt\d)(\.|$)/i.test(result) ? `_${result}` : result || '未命名';
}
function normalizeComment(c, videoId, parent = '') {
  if (!c || !c.cid) return null;
  const user = c.user || {};
  const root = String(c.reply_id && c.reply_id !== '0' ? c.reply_id : parent || '');
  const to = String(c.reply_to_reply_id && c.reply_to_reply_id !== '0' ? c.reply_to_reply_id : root);
  const timestamp = Number(c.create_time);
  return { id: String(c.cid), videoId, rootId: root, parentId: to, level: root ? 2 : 1,
    text: String(c.text || ''), nickname: String(user.nickname || ''), userId: String(user.uid || ''),
    createdAt: timestamp > 0 && timestamp < 1e11 ? new Date(timestamp * 1000).toISOString() : '',
    likes: Number(c.digg_count) || 0, replyCount: Number(c.reply_comment_total) || 0,
    region: String(c.ip_label || ''), images: (c.image_list || []).flatMap(i => (i.origin_url?.url_list || i.url_list || []).slice(0, 1)) };
}
class CommentStore {
  constructor(videoId, limit, includeReplies) { this.videoId = videoId; this.limit = limit; this.includeReplies = includeReplies; this.rows = new Map(); }
  add(comments, parent = '') {
    const added = [];
    for (const c of comments || []) {
      if (c.aweme_id && String(c.aweme_id) !== this.videoId) continue;
      const row = normalizeComment(c, this.videoId, parent);
      if (!row) continue;
      if ((this.includeReplies || row.level === 1) && !this.rows.has(row.id) && this.rows.size < this.limit) { this.rows.set(row.id, row); added.push(row); }
      if (this.includeReplies && c.reply_comment) added.push(...this.add(c.reply_comment, row.id));
    }
    return added;
  }
}
function findVideo(data, id, depth = 0) {
  if (!data || typeof data !== 'object' || depth > 12) return null;
  if (String(data.aweme_id || data.awemeId || '') === id && data.video) return data;
  for (const value of Object.values(data)) { const found = findVideo(value, id, depth + 1); if (found) return found; }
  return null;
}
function videoUrls(detail) {
  const v = detail?.video || {};
  const rates = [...(v.bit_rate || v.bitRateList || [])].sort((a,b) => (b.bit_rate || b.bitRate || 0) - (a.bit_rate || a.bitRate || 0));
  return [...new Set([...rates.flatMap(r => r.play_addr?.url_list || r.playAddr?.urlList || []), ...(v.play_addr?.url_list || v.playAddr?.urlList || [])])];
}
function allowedMedia(value) {
  try { const u = new URL(value); return u.protocol === 'https:' && ['douyin.com','iesdouyin.com','douyinvod.com','douyinpic.com','bytecdn.cn','byteimg.com','ibytedtos.com','bytecdn.com','amemv.com'].some(d => u.hostname === d || u.hostname.endsWith('.' + d)); } catch { return false; }
}
const columns = [['videoId','作品ID'],['id','评论ID'],['rootId','主评论ID'],['parentId','回复对象评论ID'],['level','评论层级'],['text','评论正文'],['nickname','昵称'],['createdAt','发布时间（UTC）'],['likes','点赞数'],['replyCount','回复数'],['region','IP属地'],['images','图片地址']];
function csvCell(value) { let s = Array.isArray(value) ? value.join('\n') : String(value ?? ''); if (/^[=+\-@\t\r]/.test(s)) s = "'" + s; return '"' + s.replace(/"/g, '""') + '"'; }
function writeCsv(file, rows) { fs.writeFileSync(file+'.tmp', '\ufeff' + [columns.map(c=>csvCell(c[1])).join(','), ...rows.map(r=>columns.map(c=>csvCell(r[c[0]])).join(','))].join('\r\n'), 'utf8');fs.renameSync(file+'.tmp',file); }
function atomicJson(file, value) { fs.mkdirSync(path.dirname(file), {recursive:true}); fs.writeFileSync(file + '.tmp', JSON.stringify(value,null,2)); fs.renameSync(file + '.tmp',file); }
module.exports = {parseInput,safeName,normalizeComment,CommentStore,findVideo,videoUrls,allowedMedia,columns,writeCsv,atomicJson};
