import { query } from './db.js';

const PIPELINES = {};
const STATUSES = {};
const USERS = {};
const LOSS_REASONS = {};

export async function loadAmoMetadata() {
  const token = process.env.AMOCRM_LONG_TOKEN;
  const domain = process.env.AMOCRM_DOMAIN || 'universaldecor';
  
  const pipelinesRes = await fetch(`https://${domain}.amocrm.ru/api/v4/leads/pipelines`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const pipelinesData = await pipelinesRes.json();
  
  for (const p of pipelinesData?._embedded?.pipelines || []) {
    PIPELINES[p.id] = p.name;
    for (const s of p._embedded?.statuses || []) {
      STATUSES[s.id] = s.name;
    }
  }
  
  const usersRes = await fetch(`https://${domain}.amocrm.ru/api/v4/users`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const usersData = await usersRes.json();
  
  for (const u of usersData?._embedded?.users || []) {
    USERS[u.id] = u.name;
  }
  
  try {
    const lossRes = await fetch(`https://${domain}.amocrm.ru/api/v4/leads/loss_reasons`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const lossData = await lossRes.json();
    for (const r of lossData?._embedded?.loss_reasons || []) {
      LOSS_REASONS[r.id] = r.name;
    }
  } catch (e) {}
}

export async function generateCSV() {
  if (Object.keys(PIPELINES).length === 0) {
    await loadAmoMetadata();
  }
  
  const domain = process.env.AMOCRM_DOMAIN || 'universaldecor';
  const result = await query('SELECT * FROM leads ORDER BY created_at DESC');
  
  const headers = [
    'Amo ID',
    'Nomi',
    'Voronka',
    'Etap',
    'Narx',
    'Menejer',
    'Yaratilgan',
    'Yangilangan',
    'Mahsulot',
    'Manba',
    'Viloyat',
    'Mijoz turi',
    'Maxsulot soni',
    'Qayerga',
    'Yopilgan',
    'Inkor sababi',
    'Havola'
  ];
  
  let csv = '\uFEFF' + headers.join(';') + '\n';
  
  for (const row of result.rows) {
    const raw = row.raw_payload || {};
    const lossReasonId = raw._embedded?.loss_reason?.[0]?.id;
    const lossReason = lossReasonId ? (LOSS_REASONS[lossReasonId] || raw._embedded?.loss_reason?.[0]?.name || '') : '';
    
    const values = [
      row.amo_id,
      `"${(row.name || '').replace(/"/g, '""')}"`,
      `"${PIPELINES[row.pipeline_id] || ''}"`,
      `"${STATUSES[row.status_id] || ''}"`,
      row.price || 0,
      `"${USERS[row.responsible_user_id] || ''}"`,
      row.created_at ? new Date(row.created_at).toLocaleString('ru-RU') : '',
      row.updated_at ? new Date(row.updated_at).toLocaleString('ru-RU') : '',
      `"${row.product_line || ''}"`,
      `"${row.source || ''}"`,
      `"${row.region || ''}"`,
      `"${row.client_type || ''}"`,
      `"${row.quantity || ''}"`,
      `"${row.destination || ''}"`,
      raw.closed_at ? new Date(raw.closed_at * 1000).toLocaleString('ru-RU') : '',
      `"${lossReason}"`,
      `https://${domain}.amocrm.ru/leads/detail/${row.amo_id}`
    ];
    csv += values.join(';') + '\n';
  }
  
  return csv;
}
