'use strict';

const impliedTradeTypes = new Set(['Implied leg', 'Calendar spread', 'Butterfly']);
function tradeReferences(row) {
  return [...new Set([row.id,row.order_id,row.buy_order_id,row.sell_order_id,
    ...(row.legs||[]).map(leg=>leg.order_id)].filter(Boolean).map(String))];
}
function buildHistoryIndex(rows) {
  const symbols=new Map(), types=new Map(), references=new Map(), implied=[];
  const add=(map,key,row)=>{if(!map.has(key))map.set(key,[]);map.get(key).push(row);};
  for(const row of rows) {
    add(symbols,row.symbol,row);add(types,row.type,row);
    if(impliedTradeTypes.has(row.type))implied.push(row);
    for(const id of tradeReferences(row))add(references,id,row);
  }
  return {rows,symbols,types,references,implied};
}
function selectHistory(index, {symbol='all',execution='all',query=''}={}) {
  const groups=[index.rows];
  if(symbol!=='all')groups.push(index.symbols.get(symbol)||[]);
  if(execution!=='all')groups.push(execution==='implied'?index.implied:index.types.get(execution)||[]);
  query=query.trim();
  if(query&&index.references.has(query))groups.push(index.references.get(query));
  const candidates=groups.reduce((a,b)=>a.length<=b.length?a:b);
  return candidates.filter(row=>(symbol==='all'||row.symbol===symbol)
    &&(execution==='all'||(execution==='implied'?impliedTradeTypes.has(row.type):row.type===execution))
    &&(!query||tradeReferences(row).some(id=>id.includes(query))));
}
if(typeof module!=='undefined')module.exports={buildHistoryIndex,selectHistory};
