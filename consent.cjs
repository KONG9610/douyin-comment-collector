const fs=require('node:fs');
const path=require('node:path');
const {createHash}=require('node:crypto');
const {atomicJson}=require('./core.cjs');
const guarded=new Set(['preview','enqueue','queue-start','retry','resume','profile-start','download-images','image','preview-image']);
class Consent{
 constructor(data){this.file=path.join(data,'consent.json');this.text=fs.readFileSync(path.join(__dirname,'disclaimer.txt'),'utf8');this.version='1.3';this.hash=createHash('sha256').update(this.text).digest('hex');}
 status(){let record;try{record=JSON.parse(fs.readFileSync(this.file,'utf8'));}catch{}const accepted=record?.version===this.version&&record?.hash===this.hash&&typeof record?.acceptedAt==='string'&&Number.isFinite(Date.parse(record.acceptedAt));return {version:this.version,hash:this.hash,text:this.text,accepted,acceptedAt:accepted?record.acceptedAt:null};}
 accept(value){if(value?.agreed!==true||value?.version!==this.version||value?.hash!==this.hash)throw new Error('请阅读当前版本声明并主动勾选同意。');atomicJson(this.file,{version:this.version,hash:this.hash,acceptedAt:new Date().toISOString()});return this.status();}
 assertAllowed(name){if(guarded.has(name)&&!this.status().accepted){const e=new Error('请先阅读并同意《使用须知与免责声明》。');e.code='CONSENT_REQUIRED';throw e;}}
}
module.exports={Consent,guarded};
