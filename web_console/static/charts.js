'use strict';

const chartView = {symbol:'', points:[], signature:'', loading:false, selected:-1, coordinates:[]};
function openPriceChart(symbol) {
  chartView.symbol=symbol;chartView.signature='';chartView.selected=-1;
  $('chart-symbol').innerHTML=ui.products.map(p=>`<option value="${esc(p.symbol)}">${esc(p.symbol)}</option>`).join('');
  $('chart-symbol').value=symbol;
  $('chart-title').textContent=symbol;
  $('chart-metrics').innerHTML='';
  $('price-chart').innerHTML=empty('Loading executed prices','Waiting for the local exchange…','activity');
  $('chart-hover').textContent='Move over the chart to inspect a trade';
  if(!$('chart-dialog').open)$('chart-dialog').showModal();
  refreshPriceChart();
}
$('chart-symbol').addEventListener('change',event=>openPriceChart(event.target.value));

async function refreshPriceChart() {
  if(!$('chart-dialog').open||chartView.loading)return;
  chartView.loading=true;
  const symbol=chartView.symbol;
  try {
    const response=await fetch(`/api/chart?symbol=${encodeURIComponent(symbol)}`,{cache:'no-store',signal:AbortSignal.timeout(5000)});
    const data=await response.json();
    if(!response.ok)throw new Error(data.error||'Chart unavailable');
    if(chartView.symbol!==symbol)return;
    const signature=ui.csrf+symbol+JSON.stringify(data.points);
    if(signature===chartView.signature)return;
    chartView.signature=signature;chartView.points=data.points;chartView.selected=-1;
    drawPriceChart();
  }catch(error){$('chart-hover').textContent=error.message;}
  finally{chartView.loading=false;}
}

function drawPriceChart() {
  const points=chartView.points, box=$('price-chart');
  chartView.coordinates=[];
  if(!points.length) {
    box.innerHTML=empty('No trades yet','Load the demo or trade this symbol to start its price curve.','activity');
    $('chart-metrics').innerHTML='';
    $('chart-hover').textContent='Only real executions are plotted.';
    return;
  }
  const prices=points.map(p=>p.price), times=points.map(p=>Number(p.timestamp_ns)/1e6);
  const first=prices[0], last=prices.at(-1), change=last-first;
  const lo=Math.min(...prices),hi=Math.max(...prices),pad=Math.max(1,(hi-lo)*.18);
  const yMin=lo-pad,yMax=hi+pad;
  const tMin=times[0],tMax=Math.max(tMin+1000,times.at(-1));
  const x=t=>64+708*(t-tMin)/(tMax-tMin),y=p=>260-232*(p-yMin)/(yMax-yMin);
  const color=change<0?'var(--red)':'var(--green)';
  chartView.coordinates=points.map((p,i)=>({x:x(times[i]),y:y(p.price)}));
  const line=chartView.coordinates.map((p,i)=>`${i?'L':'M'}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(' ');
  const area=line+` L${x(times.at(-1)).toFixed(2)},260 L64,260 Z`;
  let grid='';
  for(let i=0;i<=4;i++) {
    const price=yMin+(yMax-yMin)*i/4,at=y(price);
    grid+=`<line x1="64" y1="${at}" x2="772" y2="${at}"/><text x="52" y="${at+4}" text-anchor="end">${Number(price.toFixed(2))}</text>`;
  }
  for(let i=0;i<=4;i++) {
    const t=tMin+(tMax-tMin)*i/4,at=x(t);
    grid+=`<text x="${at}" y="288" text-anchor="${i===0?'start':i===4?'end':'middle'}">${timeText(new Date(t).toISOString())}</text>`;
  }
  const circles=chartView.coordinates.map(p=>`<circle cx="${p.x}" cy="${p.y}" r="3"/>`).join('');
  box.innerHTML=`<svg viewBox="0 0 800 308" preserveAspectRatio="none" role="img" aria-label="${esc(chartView.symbol)} executed prices, ${points.length} trades, from ${first} to ${last}"><g class="chart-grid">${grid}</g><path d="${area}" fill="${color}" opacity=".07"/><path d="${line}" fill="none" stroke="${color}" stroke-width="2.5" vector-effect="non-scaling-stroke"/><g fill="${color}">${circles}</g><line id="chart-guide" x1="0" y1="22" x2="0" y2="260" class="chart-guide" visibility="hidden"/><circle id="chart-highlight" r="5" fill="${color}" stroke="var(--panel)" stroke-width="2" visibility="hidden"/></svg>`;
  $('chart-metrics').innerHTML=[['Last',last],['Change',`${change>0?'+':''}${change}`],['High',hi],['Low',lo],['Trades',points.length]].map(([k,v])=>`<div><span>${k}</span><strong${k==='Change'?` style="color:${color}"`:''}>${v}</strong></div>`).join('');
  showChartPoint(points.length-1);
}

function showChartPoint(index) {
  const p=chartView.points[index],position=chartView.coordinates[index];
  if(!p||!position)return;
  chartView.selected=index;
  const guide=$('chart-guide'),dot=$('chart-highlight');
  if(!guide||!dot)return;
  guide.setAttribute('x1',position.x);guide.setAttribute('x2',position.x);guide.setAttribute('visibility','visible');
  dot.setAttribute('cx',position.x);dot.setAttribute('cy',position.y);dot.setAttribute('visibility','visible');
  $('chart-hover').textContent=`${p.timestamp.slice(11,23)} UTC · Price ${p.price} · Quantity ${p.quantity} · Trade ${p.id}`;
}
$('price-chart').addEventListener('pointermove',event=>{
  if(!chartView.coordinates.length)return;
  const rect=$('price-chart').getBoundingClientRect(), x=(event.clientX-rect.left)*800/rect.width;
  let index=0;
  chartView.coordinates.forEach((p,i)=>{if(Math.abs(p.x-x)<Math.abs(chartView.coordinates[index].x-x))index=i;});
  showChartPoint(index);
});
$('price-chart').addEventListener('keydown',event=>{
  if(!['ArrowLeft','ArrowRight'].includes(event.key)||!chartView.points.length)return;
  event.preventDefault();
  showChartPoint(Math.max(0,Math.min(chartView.points.length-1,chartView.selected+(event.key==='ArrowLeft'?-1:1))));
});
