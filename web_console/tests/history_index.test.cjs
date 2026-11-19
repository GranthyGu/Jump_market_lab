const {test}=require('node:test');
const assert=require('node:assert/strict');
const {buildHistoryIndex,selectHistory}=require('../static/history-index.js');

const direct={id:'9007199254740992',symbol:'A',type:'Direct',buy_order_id:'12345',sell_order_id:'777'};
const leg={id:'9007199254740993',symbol:'A',type:'Implied leg',buy_order_id:'12346'};
const spread={id:'8',symbol:'SP_AB',type:'Calendar spread',legs:[{order_id:'88888'},{order_id:'12346'}]};
const butterfly={id:'9',symbol:'BF_ABC',type:'Butterfly',order_id:'44444'};
const index=buildHistoryIndex([butterfly,spread,leg,direct]);
test('symbol and implied filters combine and preserve newest-first order',()=>{
  assert.deepEqual(selectHistory(index,{execution:'implied'}),[butterfly,spread,leg]);
  assert.deepEqual(selectHistory(index,{symbol:'A',execution:'implied'}),[leg]);
  assert.deepEqual(selectHistory(index,{symbol:'A',execution:'Direct'}),[direct]);
  assert.deepEqual(selectHistory(index,{symbol:'unknown'}),[]);
});
test('64-bit trade IDs remain distinct and references include spread legs',()=>{
  assert.deepEqual(selectHistory(index,{query:'9007199254740993'}),[leg]);
  assert.deepEqual(selectHistory(index,{query:'12346'}),[spread,leg]);
  assert.deepEqual(selectHistory(index,{symbol:'SP_AB',query:'88888'}),[spread]);
  assert.deepEqual(selectHistory(index,{execution:'Direct',query:'12346'}),[]);
});
